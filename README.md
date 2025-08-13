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

# Resin / Tissue Detector

This tool was developed for transcoding TEM image tiles, so there is a special integrated feature for resin/tissue detection. This was integrated into the tool because analysis has become IO bound, so it is preferable to perform the analysis during a single read of a given tile. Fruthermore, if we detect resin/film reliably, we can discard it before transcoding and copying.

You must specify the `--resin $OPTION` option during `transcode init` to activate this feature.


| Action | Description                                                                      |
|--------|----------------------------------------------------------------------------------|
| noop   | Do not even try to detect resin tiles.                                           |
| log    | Write filenames of detected resin tiles to logs/transcoder.resin.{pid}.log       |
| move   | Move resin tiles into ../resin/ relative to their current location               |
| stay   | Same as log + do not transcode the tiles (leave them in place).                  |
| delete | (NOT IMPLEMENTED) Same as log + delete the resin tile.                           |
| lossy  | (NOT IMPLEMENTED) Same as log + Compress the resin tile using lossy compression. |


# Installation

To transcode to remote locations, you will need to ensure cloud-files is configured with the appropriate secrets.

```bash
git clone https://github.com/seung-lab/tile-transcoder.git
pip install "./tile-transcoder[all]"
```


