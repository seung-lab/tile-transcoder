Tile Transcoder
===============

Tile Transcoder is a utility for bulk moving and converting 2D image tiles.

It works on a very similar principle to CloudFiles transfer, but with support for re-encoding the image files as well as handling bitstream compression.

Currently supports: `.bmp`, `.png`

# Listing 1: Create Database

```python
from transcoder import ResumableTransfer

rt = ResumableTransfer("xfer.db") # creates sqlite db

source = "file://...." # a cloudfiles cloudpath
dest = "gs://...." # a cloudfiles cloudpath, can be same as source

# populates the database with the transfer
rt.init(source, dest, paths, recompress=compression, reencode=encoding)
```

# Listing 2: Run Workers

Run one worker per a process, e.g. in SLURM.

```bash
transcode worker xfer.db --progress --lease-msec 60000
```


