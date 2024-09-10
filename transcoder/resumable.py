from typing import (
  Any, Dict, Optional, 
  Union, List, Tuple, 
  Callable, Generator, 
  Iterable, cast
)

import datetime
import io
import os
import sqlite3

from tqdm import tqdm

import cloudfiles.compression
from cloudfiles import CloudFiles
from cloudfiles.lib import sip

from .content_types import content_type
from .encoding import transcode_image

class EncodingNotSupported(Exception):
  pass

# the maximum value of a host parameter number is 
# SQLITE_MAX_VARIABLE_NUMBER, which defaults to 999 
# for SQLite versions prior to 3.32.0 (2020-05-22) or 
# 32766 for SQLite versions after 3.32.0. 
# https://www.sqlite.org/limits.html
SQLITE_MAX_PARAMS = 999

# syntax that changes between sqlite and mysql
# or easy adjustment if we ever need it
BIND = '?'
AUTOINC = "AUTOINCREMENT"
INTEGER = "INTEGER"

def now_msec():
  return int(datetime.datetime.utcnow().timestamp() * 1000)

class ResumableFileSet:
  """
  An interface to an sqlite database for starting and resuming
  resumable uploads or downloads.
  """
  def __init__(self, db_path, lease_msec=0):
    self.conn = sqlite3.connect(db_path)
    self.lease_msec = int(lease_msec)
    
    self._total = 0
    self._total_dirty = True

  def __del__(self):
    self.conn.close()

  def delete(self):
    cur = self.conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS filelist""")
    cur.execute("""DROP TABLE IF EXISTS xfermeta""")
    cur.close()

  def create(
    self, 
    src:str, 
    dest:str, 
    recompress:Optional[str] = None, 
    reencode:Optional[str] = None,
    level:Optional[int] = None,
    delete_original:bool = False,
  ):
    cur = self.conn.cursor()

    cur.execute("""DROP TABLE IF EXISTS filelist""")
    cur.execute("""DROP TABLE IF EXISTS xfermeta""")
    cur.execute("""DROP TABLE IF EXISTS stats""")

    cur.execute(f"""
      CREATE TABLE xfermeta (
        id {INTEGER} PRIMARY KEY {AUTOINC},
        source TEXT NOT NULL,
        dest TEXT NOT NULL,
        recompress TEXT NULL,
        reencode TEXT NULL,
        encoding_level {INTEGER} NULL,
        delete_original BOOLEAN DEFAULT FALSE,
        created {INTEGER} NOT NULL
      )
    """)

    cur.execute(
      "INSERT INTO xfermeta VALUES (?,?,?,?,?,?,?,?)", 
      [ 1, src, dest, recompress, reencode, delete_original, level, now_msec() ]
    )

    cur.execute(f"""
      CREATE TABLE filelist (
        id {INTEGER} PRIMARY KEY {AUTOINC},
        filename TEXT NOT NULL,
        finished {INTEGER} NOT NULL,
        lease {INTEGER} NOT NULL
      )
    """)
    cur.execute("CREATE INDEX resumableidxfin ON filelist(finished,lease)")
    cur.execute("CREATE INDEX resumableidxfile ON filelist(filename)")

    cur.execute(f"""
      CREATE TABLE stats (
        id {INTEGER} PRIMARY KEY {AUTOINC},
        key TEXT NOT NULL,
        value {INTEGER}
      )
    """)
    cur.execute(
      "INSERT INTO stats(id, key, value) VALUES (?,?,?)",
      [1, 'finished', 0]
    )

    cur.close()

  def insert(self, fname_iter):
    cur = self.conn.cursor()

    # cur.execute("PRAGMA journal_mode = MEMORY")
    # cur.execute("PRAGMA synchronous = OFF")

    for filenames in sip(fname_iter, SQLITE_MAX_PARAMS):
      bindlist = ",".join([f"({BIND},0,0)"] * len(filenames))
      cur.execute(f"INSERT INTO filelist(filename,finished,lease) VALUES {bindlist}", filenames)
      cur.execute("commit")

    cur.close()
    self._total_dirty = True

  def metadata(self):
    cur = self.conn.cursor()
    cur.execute("SELECT source, dest, recompress, reencode, encoding_level, delete_original, created FROM xfermeta LIMIT 1")
    row = cur.fetchone()

    meta = {
      "source": row[0],
      "dest": row[1],
      "recompress": row[2],
      "reencode": row[3],
      "encoding_level": row[4],
      "delete_original": row[5],
      "created": row[6],
    }

    if not meta["recompress"] or meta["recompress"] == '0':
      meta["recompress"] = None

    if not meta["reencode"] or meta["reencode"] == '0':
      meta["reencode"] = None

    if not meta["encoding_level"] or meta["encoding_level"] == '0':
      meta["encoding_level"] = None    

    return meta

  def mark_finished(self, fname_iter):
    cur = self.conn.cursor()

    for filenames in sip(fname_iter, SQLITE_MAX_PARAMS):
      bindlist = ",".join([f"{BIND}"] * len(filenames))
      cur.execute(f"UPDATE filelist SET finished = 1 WHERE filename in ({bindlist})", filenames)
      cur.execute(f"UPDATE stats SET value = value + {len(filenames)} WHERE id = 1")
      cur.execute("commit")
    cur.close()

  def next(self, limit=None, block_size=200):
    cur = self.conn.cursor()

    N = 0

    while True:
      ts = now_msec()
      cur.execute(f"""SELECT filename FROM filelist WHERE finished = 0 AND lease <= {ts} LIMIT {int(block_size)}""")
      rows = cur.fetchmany(block_size)
      N += len(rows)
      if len(rows) == 0:
        break
      
      filenames = [ x[0] for x in rows ]
      bindlist = ",".join([f"{BIND}"] * len(filenames))
      ts = now_msec() + self.lease_msec
      cur.execute(f"UPDATE filelist SET lease = {ts} WHERE filename in ({bindlist})", filenames)
      cur.execute("commit")

      yield from filenames
      
      if limit and N >= limit:
        break

    cur.close()

  def _scalar_query(self, sql:str) -> int:
    cur = self.conn.cursor()
    cur.execute(sql)
    res = cur.fetchone()
    cur.close()
    return int(res[0])

  def total(self):
    """Returns the total number of tasks (both processed and unprocessed)."""
    if not self._total_dirty:
      return self._total

    self._total = self._scalar_query(f"SELECT max(id) FROM filelist")
    self._total_dirty = False
    return self._total

  def finished(self):
    return self._scalar_query(f"SELECT value FROM stats WHERE id = 1")

  def remaining(self):
    return self.total() - self.finished()

  def num_leased(self):
    ts = int(now_msec())
    return self._scalar_query(
      f"SELECT count(filename) FROM filelist WHERE finished = 0 AND lease > {ts}"
    )

  def available(self):
    ts = int(now_msec())
    return self._scalar_query(
      f"SELECT count(filename) FROM filelist WHERE finished = 0 AND lease <= {ts}"
    )

  def release(self):
    cur = self.conn.cursor()
    cur.execute(f"UPDATE filelist SET lease = 0")
    cur.execute("commit")
    cur.close()

  def __len__(self):
    return self.remaining()

  def __iter__(self):
    return self.next()

class ResumableTransfer:
  def __init__(self, db_path, lease_msec=0):
    self.db_path = db_path
    self.rfs = ResumableFileSet(db_path, lease_msec)

  def __len__(self):
    return len(self.rfs)

  def _normalize_compression(self, recompress, reencode):
    # disable bitstream compression unless the 
    # format supports uncompressed output. 
    # e.g. for png, jpeg etc do not apply bitstream compression
    if reencode in ('bmp', 'tiff'):
      return (recompress, reencode)
    else:
      return (False, reencode)

  def init(self, src, dest, paths=None, recompress=None, reencode=None, delete_original=False):
    if isinstance(paths, str):
      paths = list(CloudFiles(paths))
    elif isinstance(paths, CloudFiles):
      paths = list(paths)
    elif paths is None:
      paths = list(CloudFiles(src))

    (recompress, reencode) = self._normalize_compression(recompress, reencode)

    self.rfs.create(src, dest, recompress, reencode, delete_original)
    self.rfs.insert(paths)

  def execute(self, progress=False, block_size=200):
    meta = self.rfs.metadata()

    cf_src = CloudFiles(meta["source"])
    cf_dest = CloudFiles(meta["dest"])

    total = self.rfs.total()
    pbar = tqdm(total=total, desc="Transfer", disable=(not progress))
    pbar.n = total - self.rfs.remaining()

    with pbar:
      pbar.refresh()
      for paths in sip(self.rfs, block_size):
        if meta["reencode"] is None:
          cf_src.transfer_to(meta["dest"], paths=paths, reencode=meta["recompress"])
        else:
          files = cf_src.get(paths, return_dict=True)

          reencoded = []
          original_filenames = []
          for filename, binary in files.items():
            original_filenames.append(filename)
            new_filename, new_binary = transcode_image(filename, binary, meta["reencode"], meta["encoding_level"])
            reencoded.append({
              "path": new_filename,
              "content": new_binary,
              "raw": False,
              "content_type": content_type(meta["reencode"]),
              "compress": None,
            })

          if meta["recompress"]:
            reencoded = cloudfiles.compression.transcode(
              reencoded, meta["recompress"], in_place=True
            )

          cf_dest.puts(reencoded, raw=True)

        if meta["delete_original"]:
          cf_src.delete(original_filenames)
        
        self.rfs.mark_finished(paths)
        
        pbar.n = total - self.rfs.remaining()
        pbar.refresh()

  def close(self):
    self.rfs.delete()
    try:
      os.remove(self.db_path)
    except FileNotFoundError:
      pass


