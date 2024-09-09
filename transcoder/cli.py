import click

from cloudfiles.paths import get_protocol

from .resumable import ResumableTransfer

SUPPORTED_ENCODINGS = set([ 'bmp', 'png' ])

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
@click.argument("source", nargs=1)
@click.option('-e', '--encoding', default='same', help="Destination encoding type. Options: same, png", show_default=True)
@click.option('-c', '--compression', required=True, default='same', help="Destination compression type. Options: same, none, gzip, br, zstd", show_default=True)
@click.option('-l', '--level', default=None, type=int, help="Encoding level for jpeg (0-100),jpegxl (0-100, 100=lossless),png (0-9).", show_default=True)
@click.option('--db', default=None, required=True, help="Filepath of the sqlite database used for tracking progress. Different databases should be used for each job.")
def xferinit(source, encoding, compression, db, level):
  """(1) Create db of files from the source."""
  if compression == "same":
    compression = None
  elif compression == "none":
    compression = False

  if encoding == "same":
    encoding = None

  source = normalize_path(source)
  destination = normalize_path(destination)

  rt = ResumableTransfer(db)
  rt.init(source, source, source, recompress=compression, reencode=encoding, level=level)

@cli_main.command("worker")
@click.argument("db")
@click.option('--progress', is_flag=True, default=False, help="Show transfer progress.")
@click.option('--lease-msec', default=0, help="(distributed transfers) Number of milliseconds to lease each task for.", show_default=True)
@click.option('-b', '--block-size', default=200, help="Number of files to process at a time.", show_default=True)
def worker(db, progress, lease_msec, block_size):
  """(2) Perform the transfer using the database.

  Multiple clients can use the same database
  for execution.
  """
  rt = ResumableTransfer(db, lease_msec)
  rt.execute(progress=progress, block_size=block_size)
  rt.close()

@cli_main.command("status")
@click.argument("db")
def status(db):
  """Print how many tasks are enqueued."""
  rt = ResumableTransfer(db)
  total = rt.rfs.total()
  remaining = rt.rfs.remaining()
  print(f"{remaining} remaining ({remaining/total*100.0:.2f}%)")
  print(f"{total} total")

@cli_main.command("release")
@click.argument("db")
def release(db):
  """Release all leased tasks to the available pool."""
  rt = ResumableTransfer(db)
  rt.rfs.release()




