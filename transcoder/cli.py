import click
import os.path
import time

import multiprocessing as mp
from tqdm import tqdm

from cloudfiles import CloudFiles
from cloudfiles.paths import get_protocol
from cloudfiles.lib import toabs

from .resumable import ResumableTransfer

mp.set_start_method("spawn", force=True)

def normalize_path(cloudpath):
  if not get_protocol(cloudpath):
    return "file://" + toabs(cloudpath)
  return cloudpath

@click.group("main")
def cli_main():
  """
  Create named resumable transfers.

  This is a more reliable version of
  the cp command for large transfers.

  Resumable transfers can be performed
  in parallel by multiple clients. They
  work by saving filenames to a sqlite3
  database and checking them off.

  To use run:

  1. transcode init ... --in-place --db NAME

  2. transcode execute NAME
  """
  pass

@cli_main.command("init")
@click.argument("source", required=True)
@click.argument("destination", required=False)
@click.option('--encoding', default='same', help="Destination encoding type. Options: same, jpeg, jxl, png, bmp, tiff", show_default=True)
@click.option('--compression', required=True, default='same', help="Destination compression type. Options: same, none, gzip, br, zstd", show_default=True)
@click.option('--level', default=None, type=int, help="Encoding level for jpeg (0-100),jpegxl (0-100, 100=lossless),png (0-9).", show_default=True)
@click.option('--jxl-effort', default=3, type=int, help="(jpegxl) Set effort for jpegxl encoding 1-10.", show_default=True)
@click.option('--jxl-decoding-speed', default=0, type=int, help="(jpegxl) Prioritize faster decoding 0-4 (0: default).", show_default=True)
@click.option('--delete-original', default=False, is_flag=True, help="Deletes the original file after transcoding.", show_default=True)
@click.option('--ext', default=None, help="If present, filter files for this extension.")
@click.option('--db', default=None, required=True, help="Filepath of the sqlite database used for tracking progress. Different databases should be used for each job.")
def xferinit(
  source, destination, 
  encoding, compression, 
  db, level, 
  delete_original, ext,
  jxl_effort, jxl_decoding_speed,
):
  """(1) Create db of files from the source."""
  if compression == "same":
    compression = None
  elif compression == "none":
    compression = False

  encoding = encoding.lower()

  if encoding == "same":
    encoding = None
  elif encoding == "jxl":
    encoding = "jpegxl"

  encoding_options = {}

  if encoding == "jpegxl":
    encoding_options["effort"] = int(jxl_effort)
    encoding_options["decodingspeed"] = int(jxl_decoding_speed)

  source = normalize_path(source)

  if destination is None:
    destination = source
  else:
    destination = normalize_path(destination)

  paths = CloudFiles(source).list()
  if ext:
    paths = ( p for p in paths if p.endswith(f'.{ext}') )

  rt = ResumableTransfer(db)
  rt.init(
    source, destination, paths,
    recompress=compression,
    reencode=encoding, 
    level=level, 
    delete_original=delete_original,
    encoding_options=encoding_options,
  )

def _do_work(db, lease_msec, db_timeout, block_size, verbose):
  rt = ResumableTransfer(db, lease_msec, db_timeout=db_timeout)
  rt.execute(progress=False, block_size=block_size, verbose=verbose)

@cli_main.command("worker")
@click.argument("db")
@click.option('-p', '--parallel', default=1, type=int, help="Number of workers.")
@click.option('--progress', is_flag=True, default=False, help="Show transfer progress.")
@click.option('--lease-msec', default=0, help="(distributed transfers) Number of milliseconds to lease each task for.", show_default=True)
@click.option('-b', '--block-size', default=200, help="Number of files to process at a time.", show_default=True)
@click.option('--verbose', is_flag=True, default=False, help="Print more about what the worker is doing.", show_default=True)
@click.option('--db-timeout', default=5.0, type=float, help="How many seconds to wait when the SQLite DB is locked. Use higher values under multi-process contention.", show_default=True)
@click.option('--ramp-sec', default=0.0, type=float, help="How many seconds to wait between launching additional processes.", show_default=True)
@click.option('--cleanup', is_flag=True, default=False, help="Delete the database when finished.")
def worker(
  db, progress, 
  lease_msec, block_size, 
  verbose, db_timeout, 
  cleanup, parallel,
  ramp_sec,
):
  """(2) Perform the transfer using the database.

  Multiple clients can use the same database
  for execution.
  """
  assert parallel > 0 and int(parallel) == parallel
  assert block_size > 0
  assert lease_msec >= 0

  if parallel > 1 and lease_msec == 0:
    print("Parallel workers require you to set lease_msec to avoid highly duplicated work.")
    return

  if not os.path.exists(db):
    print(f"Database {db} does not exist. Did you call transcode init?")
    return
  
  rt = ResumableTransfer(db, lease_msec, db_timeout=db_timeout)

  remaining = len(rt)
  total = rt.rfs.total()
  completed = total - remaining

  pbar = tqdm(
    desc="Tiles Transcoded", 
    total=total,
    initial=completed,
    disable=(not progress),
  )

  def update_pbar():
    remaining = len(rt)
    completed = total - remaining
    pbar.n = completed
    pbar.refresh()
    return remaining

  processes = []
  for _ in range(parallel):
    p = mp.Process(
      target=_do_work, 
      args=(db, lease_msec, db_timeout, block_size, verbose)
    )
    p.start()
    if ramp_sec > 0:
      time.sleep(ramp_sec)
      update_pbar()
    processes.append(p)

  while update_pbar() > 0:
    time.sleep(0.5)

  for p in processes:
    p.join()

  pbar.close()

  if cleanup:
    rt.close()

@cli_main.command("status")
@click.argument("db")
def status(db):
  """Print how many tasks are enqueued."""
  rt = ResumableTransfer(db)
  total = rt.rfs.total()
  remaining = rt.rfs.remaining()
  completed = total - remaining
  leased = rt.rfs.num_leased()
  errors = rt.rfs.num_errors()
  print(f"{remaining} remaining ({remaining/total*100.0:.2f}%)")
  print(f"{completed} completed ({completed/total*100.0:.2f}%)")
  print(f"{leased} leased ({leased/total*100.0:.2f}%)")
  print(f"{errors} errors ({errors/total*100.0:.2f}%)")
  print(f"{total} total")

@cli_main.command("release")
@click.argument("db")
def release(db):
  """Release all leased tasks to the available pool."""
  rt = ResumableTransfer(db)
  rt.rfs.release()




