from .resumable import ResumableTransfer
from .cli import cli_main


# def in_place(src:str, db_path:str, paths:list):
#   xfer = ResumableTransfer(db_path, lease_msec=60000)
#   xfer.init(src, src, paths=paths, reencode='png')

# def worker(db_path, lease_msec=60000):
#   xfer = ResumableTransfer(db_path, lease_msec=lease_msec)
#   xfer.execute(progress=True)






