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
import sys

from tqdm import tqdm

import cloudfiles.compression
from cloudfiles import CloudFiles, CloudFile
from cloudfiles.lib import sip

from .detectors import ResinHandling, make_resin_action
from .content_types import content_type
from .encoding import transcode_image
from .exceptions import SkipTranscoding

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
  def __init__(
    self, 
    db_path:str, 
    lease_msec:int = 0, 
    timeout:float = 5.0,
    default_reservation:int = 200,
  ):
    self.conn = sqlite3.connect(db_path, timeout=timeout)
    self.lease_msec = int(lease_msec)
    self.default_reservation = int(default_reservation)

    self._total = 0
    self._total_dirty = True

  def __del__(self):
    self.conn.close()

  def delete(self):
    cur = self.conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS filelist""")
    cur.execute("""DROP TABLE IF EXISTS xfermeta""")
    cur.execute("""DROP TABLE IF EXISTS stats""")
    cur.execute("""DROP TABLE IF EXISTS errors""")
    cur.close()

  def create(
    self, 
    src:str, 
    dest:str, 
    recompress:Optional[str] = None, 
    reencode:Optional[str] = None,
    level:Optional[int] = None,
    delete_original:bool = False,
    resin_handling:int = ResinHandling.NOOP,
    encoding_options:Dict[str,int] = {}
  ):
    cur = self.conn.cursor()

    cur.execute("""DROP TABLE IF EXISTS filelist""")
    cur.execute("""DROP TABLE IF EXISTS xfermeta""")
    cur.execute("""DROP TABLE IF EXISTS stats""")
    cur.execute("""DROP TABLE IF EXISTS errors""")

    cur.execute(f"""
      CREATE TABLE xfermeta (
        id {INTEGER} PRIMARY KEY {AUTOINC},
        source TEXT NOT NULL,
        dest TEXT NOT NULL,
        recompress TEXT NULL,
        reencode TEXT NULL,
        encoding_level {INTEGER} NULL,
        encoding_options TEXT NULL,
        resin_handling {INTEGER} DEFAULT {int(ResinHandling.NOOP)},
        delete_original BOOLEAN DEFAULT FALSE,
        created {INTEGER} NOT NULL
      )
    """)

    encoding_options_serialized = ";".join([
      f"{k}={int(v)}"
      for k,v in encoding_options.items()
    ])

    fields = [
      "id", 
      "source", 
      "dest", 
      "recompress", 
      "reencode", 
      "encoding_level", 
      "encoding_options", 
      "resin_handling",
      "delete_original", 
      "created"
    ]

    cur.execute(
      f"""INSERT INTO xfermeta 
      ({",".join(fields)}) 
      VALUES ({",".join(["?"] * len(fields))})""", 
      [ 
        1, src, dest, 
        recompress, reencode, level, 
        encoding_options_serialized, int(resin_handling), delete_original, 
        now_msec()
      ]
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
      CREATE TABLE errors (
        id {INTEGER} PRIMARY KEY {AUTOINC},
        filename TEXT NOT NULL,
        error TEXT NOT NULL,
        created {INTEGER} NOT NULL
      )
    """)

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

  def errors(self, n=1000):
    cur = self.conn.cursor()
    cur.execute(f"SELECT filename, error, created FROM errors LIMIT {int(n)}")
    results = cur.fetchmany()
    cur.close()
    return results

  def record_error(self, filename, error):
    cur = self.conn.cursor()
    cur.execute(
      "INSERT INTO errors (filename, error, created) VALUES (?,?,?)",
      [filename, str(error), now_msec()]
    )
    cur.execute(
      "UPDATE filelist SET finished = 2 WHERE filename = ?", 
      [filename]
    )
    cur.close()

  def insert(self, fname_iter) -> int:
    cur = self.conn.cursor()

    # cur.execute("PRAGMA journal_mode = MEMORY")
    # cur.execute("PRAGMA synchronous = OFF")

    count = 0

    for filenames in sip(fname_iter, SQLITE_MAX_PARAMS):
      count += len(filenames)
      bindlist = ",".join([f"({BIND},0,0)"] * len(filenames))
      cur.execute(f"INSERT INTO filelist(filename,finished,lease) VALUES {bindlist}", filenames)
      cur.execute("commit")

    cur.close()
    self._total_dirty = True

    return count

  def metadata(self):
    cur = self.conn.cursor()

    fields = [
        "source",
        "dest",
        "recompress",
        "reencode", 
        "encoding_level",
        "encoding_options",
        "resin_handling",
        "delete_original",
        "created"
    ]

    cur.execute(f"""
      SELECT 
        {",".join(fields)}
      FROM xfermeta 
      LIMIT 1
    """)
    row = cur.fetchone()

    meta = {
      field: row[i]
      for i, field in enumerate(fields)
    }

    meta["resin_handling"] = int(meta["resin_handling"])
    meta["delete_original"] = bool(meta["delete_original"])

    if not meta["recompress"] or meta["recompress"] == '0':
      meta["recompress"] = None

    if not meta["reencode"] or meta["reencode"] == '0':
      meta["reencode"] = None

    if not meta["encoding_level"] or meta["encoding_level"] == '0':
      meta["encoding_level"] = None

    if not meta["encoding_options"] or meta["encoding_options"] == '0':
      meta["encoding_options"] = {}
    else:
      meta["encoding_options"] = meta["encoding_options"].split(";")
      meta["encoding_options"] = {
        pair.split("=")[0]: int(pair.split("=")[1])
        for pair in meta["encoding_options"]
      }

    return meta

  def mark_finished(self, fname_iter):
    cur = self.conn.cursor()

    for filenames in sip(fname_iter, SQLITE_MAX_PARAMS):
      bindlist = ",".join([f"{BIND}"] * len(filenames))
      cur.execute(f"UPDATE filelist SET finished = 1 WHERE filename in ({bindlist})", filenames)
      cur.execute(f"UPDATE stats SET value = value + {len(filenames)} WHERE id = 1")
      cur.execute("commit")
    cur.close()

  def next(self, limit=None, reservation=None):
    cur = self.conn.cursor()

    if reservation is None:
      reservation = self.default_reservation

    N = 0

    ts = now_msec()

    while True:
      cur.execute("BEGIN EXCLUSIVE TRANSACTION")
      cur.execute(f"""SELECT filename FROM filelist WHERE finished = 0 AND lease < {ts} LIMIT {int(reservation)}""")
      rows = cur.fetchmany(reservation)
      N += len(rows)
      if len(rows) == 0:
        break
      
      filenames = [ x[0] for x in rows ]
      bindlist = ",".join([f"{BIND}"] * len(filenames))
      lease_msec = now_msec() + self.lease_msec
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

  def total(self) -> int:
    """Returns the total number of tasks (both processed and unprocessed)."""
    if not self._total_dirty:
      return self._total

    self._total = self._scalar_query(f"SELECT max(id) FROM filelist")
    self._total_dirty = False
    return self._total

  def finished(self) -> int:
    return self._scalar_query(f"SELECT value FROM stats WHERE id = 1")

  def remaining(self) -> int:
    return self.total() - self.finished()

  def num_leased(self) -> int:
    ts = int(now_msec())
    return self._scalar_query(
      f"SELECT count(filename) FROM filelist WHERE finished = 0 AND lease > {ts}"
    )

  def num_errors(self) -> int:
    return self._scalar_query(f"SELECT count(*) from errors")

  def has_errors(self) -> bool:
    return self._scalar_query(f"SELECT count(*) from errors limit 1") > 0

  def available(self) -> int:
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
  def __init__(self, db_path:str, lease_msec:int = 0, db_timeout:float = 5.0):
    self.db_path = db_path
    self.rfs = ResumableFileSet(db_path, lease_msec, timeout=db_timeout)

  def __len__(self) -> int:
    return len(self.rfs)

  def _normalize_compression(self, recompress, reencode):
    # disable bitstream compression unless the 
    # format supports uncompressed output. 
    # e.g. for png, jpeg etc do not apply bitstream compression
    if reencode in ('bmp', 'tiff'):
      return (recompress, reencode)
    else:
      return (False, reencode)

  def init(
    self, 
    src:str, 
    dest:str, 
    paths:Optional[str] = None, 
    recompress:Optional[str] = None, 
    reencode:Optional[str] = None, 
    resin_handling:int = ResinHandling.NOOP,
    delete_original:bool = False, 
    level:Optional[int] = None,
    encoding_options:dict = {},
  ) -> int:
    if isinstance(paths, str):
      paths = CloudFiles(paths).list()
    elif isinstance(paths, CloudFiles):
      paths = paths.list()
    elif paths is None:
      paths = CloudFiles(src).list()

    (recompress, reencode) = self._normalize_compression(recompress, reencode)

    self.rfs.create(
      src, dest, 
      recompress, reencode,
      level=level,
      resin_handling=resin_handling,
      delete_original=delete_original, 
      encoding_options=encoding_options,
    )
    return self.rfs.insert(paths)

  def execute(
    self, 
    progress:bool = False, 
    block_size:int = 200,
    verbose:bool = False,
    codec_threads:int = 0,
  ):
    """
    Start working through tasks in the database.

    progress: show progress bar
    block_size: how many items to download and process at once
    timeout: how long to wait for a sqlite lock to release
    verbose: print what the worker is doing
    codec_threads: for codecs that are multithreaded, use this
      number of threads. 0 = num cores
    """
    self.rfs.default_reservation = block_size

    meta = self.rfs.metadata()
    meta["encoding_options"]["num_threads"] = int(codec_threads)

    cf_src = CloudFiles(meta["source"], progress=bool(verbose))
    cf_dest = CloudFiles(meta["dest"])

    if verbose:
      print(f"Executing PID {os.getpid()}...")

    total = self.rfs.total()
    pbar = tqdm(
      initial=self.rfs.finished(), 
      total=total, 
      desc="Transfer", 
      disable=(not progress)
    )

    resin_callback = make_resin_action(meta["source"], verbose, meta["resin_handling"])

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
            if binary in (None, b''):
              if verbose:
                print(f"{filename} is missing.")
              self.rfs.record_error(filename, "missing file.")
              continue

            try:
              if verbose:
                print(f"{filename} to {meta['reencode']}")

              new_filename, new_binary = transcode_image(
                filename, binary, 
                meta["reencode"], meta["encoding_level"],
                callback=resin_callback,
                **meta["encoding_options"]
              )
            except SkipTranscoding:
              continue
            except KeyboardInterrupt:
              raise
            except Exception as err:
              if verbose:
                print(f"{filename} error: {err}")
              self.rfs.record_error(filename, err)
              continue
            
            original_filenames.append(filename)
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
        
        pbar.n = self.rfs.finished()
        pbar.refresh()

  def close(self):
    if self.rfs.has_errors():
      print("There were errors during processing. Keeping the database intact.", file=sys.stderr)
      return

    self.rfs.delete()
    try:
      os.remove(self.db_path)
    except FileNotFoundError:
      pass


