Tile Transcoder
===============

Tile Transcoder is a utility for bulk moving and converting 2D image tiles.

It works on a very similar principle to CloudFiles transfer, but with support for re-encoding the image files as well as handling bitstream compression.

Currently supports: `.bmp`, `.png`, `.jpeg`, `.jxl`, `.tiff`

# Listing 1: Create Database

```bash
transcode init $SRC $DEST --level 100 --encoding jxl --jxl-effort 2 --db xfer.db
```

# Listing 2: Run Workers

Run two workers per a process, e.g. in SLURM.

```bash
transcode worker xfer.db --parallel 2 --progress --lease-msec 60000 --block-size 20
```


