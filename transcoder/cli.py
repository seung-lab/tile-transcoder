import click
import multiprocessing as mp
import os.path
import time

from tqdm import tqdm

from cloudfiles import CloudFiles
from cloudfiles.paths import get_protocol
from cloudfiles.lib import toabs

from .encoding import SUPPORTED_ENCODINGS
from .detectors import ResinHandling
from .resumable import ResumableTransfer

mp.set_start_method("spawn", force=True)

def SI(val):
  if val < 1024:
    return f"{val} bytes"
  elif val < 2**20:
    return f"{(val / 2**10):.2f} KiB"
  elif val < 2**30:
    return f"{(val / 2**20):.2f} MiB"
  elif val < 2**40:
    return f"{(val / 2**30):.2f} GiB"
  elif val < 2**50:
    return f"{(val / 2**40):.2f} TiB"
  elif val < 2**60:
    return f"{(val / 2**50):.2f} PiB"
  else:
    return f"{(val / 2**60):.2f} EiB"

def normalize_path(cloudpath:str) -> str:
  if not get_protocol(cloudpath):
    return "file://" + toabs(cloudpath)
  return cloudpath

def natural_time_delta(seconds:float) -> str:
  sec = abs(seconds)
  if sec == 0.0:
    return f"just now"
  elif sec < 60:
    return f"{int(sec)} seconds {'from now' if seconds > 0 else 'ago'}"
  elif sec < 3600:
    minutes = int(sec / 60)
    return f"{minutes} minutes {'from now' if seconds > 0 else 'ago'}"
  elif sec < 86400:
    hours = int(sec / 3600)
    return f"{hours} hours {'from now' if seconds > 0 else 'ago'}"
  elif sec < 86400 * 365 * 80:
    days = int(sec / 86400)
    return f"{days} days {'from now' if seconds > 0 else 'ago'}"
  else:
    return f"never"

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
@click.option('--ext', default=None, help="If present, filter files for these comma separated extensions.")
@click.option('--db', default=None, required=True, help="Filepath of the sqlite database used for tracking progress. Different databases should be used for each job.")
@click.option('--resin', default="noop", help="Uses a tissue detector tuned for TEM to check if a tile has tissue. Possible actions: noop, log, move, stay. move: put tile in the source directory under 'resin'. stay: log + skip copying the tile.", show_default=True)
def xferinit(
  source, destination, 
  encoding, compression, 
  db, level, 
  delete_original, ext,
  jxl_effort, jxl_decoding_speed,
  resin,
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
  elif encoding == "jpg":
    encoding = "jpeg"

  if encoding not in SUPPORTED_ENCODINGS:
    print(f"{encoding} is not a supported encoding.")
    return

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
    ext = ext.split(',')
    paths = ( 
      p for p in paths 
      if any(p.endswith(f'.{e}') for e in ext)
    )

  resin_handling = ResinHandling.NOOP
  if resin == "move":
    resin_handling = ResinHandling.MOVE
  elif resin == "log":
    resin_handling = ResinHandling.LOG
  elif resin == "stay":
    resin_handling = ResinHandling.STAY

  rt = ResumableTransfer(db)
  inserted = rt.init(
    source, destination, paths,
    recompress=compression,
    reencode=encoding, 
    level=level, 
    resin_handling=resin_handling,
    delete_original=delete_original,
    encoding_options=encoding_options,
  )

  if inserted == 0:
    print("WARNING: No files inserted into the database. Is your filter extension correct?")

def _do_work(db, progress, lease_msec, db_timeout, block_size, verbose, codec_threads):
  try:
    rt = ResumableTransfer(db, lease_msec, db_timeout=db_timeout)
    rt.execute(progress=progress, block_size=block_size, verbose=verbose, codec_threads=codec_threads)
  except KeyboardInterrupt:
    pass

@cli_main.command("worker")
@click.argument("db")
@click.option('-p', '--parallel', default=1, type=int, help="Number of workers.")
@click.option('--progress', is_flag=True, default=False, help="Show transfer progress.")
@click.option('--lease-msec', default=0, help="(distributed transfers) Number of milliseconds to lease each task for.", show_default=True)
@click.option('-b', '--block-size', default=200, help="Number of files to process at a time.", show_default=True)
@click.option('--verbose', is_flag=True, default=False, help="Print more about what the worker is doing.", show_default=True)
@click.option('--db-timeout', default=5.0, type=float, help="How many seconds to wait when the SQLite DB is locked. Use higher values under multi-process contention.", show_default=True)
@click.option('--ramp-sec', default=0.25, type=float, help="How many seconds to wait between launching additional processes.", show_default=True)
@click.option('--cleanup', is_flag=True, default=False, help="Delete the database when finished.")
@click.option('--codec-threads', default=0, type=int, help="For codecs that support multiple threads, use this number of threads (0 = num cores). Supported codecs: jxl", show_default=True)
def worker(
  db, progress, 
  lease_msec, block_size, 
  verbose, db_timeout, 
  cleanup, parallel,
  ramp_sec, codec_threads,
):
  """(2) Perform the transfer using the database.

  Multiple clients can use the same database
  for execution.
  """
  assert parallel > 0 and int(parallel) == parallel
  assert block_size > 0
  assert lease_msec >= 0
  assert codec_threads >= 0

  if not os.path.exists(db):
    print(f"Database {db} does not exist. Did you call transcode init?")
    return

  if parallel == 1:
    _do_work(db, progress, lease_msec, db_timeout, block_size, verbose, codec_threads)
    return

  if parallel > 1 and lease_msec == 0:
    print("Parallel workers require you to set lease_msec to avoid highly duplicated work.")
    return
  
  rt = ResumableTransfer(db, lease_msec, db_timeout=db_timeout)

  remaining = rt.rfs.remaining()
  total = rt.rfs.total()
  completed = total - remaining

  pbar = tqdm(
    desc="Tiles Transcoded", 
    total=total,
    initial=completed,
    disable=(not progress),
  )

  def update_pbar():
    remaining = rt.rfs.remaining()
    completed = total - remaining
    pbar.n = completed
    pbar.refresh()
    return remaining

  processes = []
  for _ in range(parallel):
    p = mp.Process(
      target=_do_work, 
      args=(db, False, lease_msec, db_timeout, block_size, verbose, codec_threads)
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
@click.option('--eta', default=0.0, type=float, help="Measure task rate and ETA over this many seconds if > 0.", show_default=True)
@click.option('--raw-counts', is_flag=True, default=False, help="Switch off human readability for counts.", show_default=True)
def status(db, eta, raw_counts):
  """Print how many tasks are enqueued."""
  rt = ResumableTransfer(db)

  s = time.monotonic()

  total = rt.rfs.total()
  remaining = rt.rfs.remaining()
  completed = total - remaining
  leased = rt.rfs.num_leased()
  errors = rt.rfs.num_errors()

  original_bytes_processed = rt.rfs.original_bytes_processed()
  transcoded_bytes_processed = rt.rfs.transcoded_bytes_processed()

  ratio = 1.0
  if original_bytes_processed != 0:
    ratio = transcoded_bytes_processed / original_bytes_processed

  if eta > 0 and remaining > 0:
    time.sleep(eta)
    remaining2 = rt.rfs.remaining()
    completed2 = total - remaining2
    e = time.monotonic()

  raw_counts_fn = lambda x: f"{x} bytes"
  dispfn = raw_counts_fn if raw_counts else SI

  print(f"{remaining} remaining ({remaining/total*100.0:.2f}%)")
  print(f"{completed} completed ({completed/total*100.0:.2f}%)")
  print(f"{leased} leased ({leased/total*100.0:.2f}%)")
  print(f"{errors} errors ({errors/total*100.0:.2f}%)")
  print(f"{total} total files")
  print(f"{dispfn(original_bytes_processed)} original")
  print(f"{dispfn(transcoded_bytes_processed)} transcoded ({ratio*100.0:.2f}%)")

  if eta > 0 and remaining > 0:
    elapsed = e - s

    if remaining2 == 0:
      print("finished.")
      return

    rate = (completed2 - completed) / elapsed
    prediction = float('inf')
    if rate != 0:
      prediction = remaining2 / rate
    print('--')
    print(f"{rate:.1f} tiles per sec.")
    print(f"done in {natural_time_delta(prediction)}")

@cli_main.command("release")
@click.argument("db")
def release(db):
  """Release all leased tasks to the available pool."""
  rt = ResumableTransfer(db)
  rt.rfs.release()




