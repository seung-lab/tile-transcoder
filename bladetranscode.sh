#!/bin/bash
# assumes SOURCE looks like
# .../bladeseq-2025.02.07-16.47.17/
# DEST looks like
# /some/directory/

# dependencies:
# tile transcoder: https://github.com/seung-lab/tile-transcoder
#  (install Pillow and imagecodecs too)
# requires cloud-files>=5.8.0

# bladetranscode /mnt/sink/scratch/yannan/.../bladeseq-2025.02.07-16.47.17/ /mnt/sink/scratch/yannan/.../jxl/

SOURCE=$1
DEST=$2

BSEQ=$(basename $SOURCE)
SECTION=$(ls $SOURCE)
SOURCE="$SOURCE/$SECTION"

DEST="$DEST/$BSEQ/$SECTION"

cloudfiles mkdir $DEST

cloudfiles mkdir $HOME/transcode_dbs/
DBNAME=$HOME/transcode_dbs/transcode-$(date +%s).db

cloudfiles cp $SOURCE/*.jpg $DEST/
cloudfiles cp -r $SOURCE/metadata $DEST/
cloudfiles cp -r $SOURCE/montage $DEST/
cloudfiles mkdir $DEST/subtiles/metadata/
cloudfiles cp -r $SOURCE/subtiles/metadata/ $DEST/subtiles/

echo "Database: $DBNAME"
transcode init "$SOURCE/subtiles" "$DEST/subtiles" --db $DBNAME --ext bmp --encoding jxl --compression none --level 100 --jxl-effort 2 --jxl-decoding-speed 0
transcode worker --parallel 7 -b 1 --codec-threads 4 --lease-msec 60000 --db-timeout 10000 --ramp-sec 0.25 $DBNAME --progress --cleanup

bmp_files=$(find "$SOURCE/subtiles" -type f -name '*.bmp' -printf '%f\n' | sed 's/\.bmp$//' | sort)
jxl_files=$(find "$DEST/subtiles" -type f -name '*.jxl' -printf '%f\n' | sed 's/\.jxl$//' | sort)

if [ "$bmp_files" != "$jxl_files" ]; then
    echo "Error: Directory contents differ:"
    echo "$diff_result"
    exit 1
fi


get_file_size() {
    local file="$1"
    # Try GNU stat first, then BSD stat, then fallback to wc
    if size=$(stat -c %s "$file" 2>/dev/null); then
        echo "$size"
    elif size=$(stat -f %z "$file" 2>/dev/null); then
        echo "$size"
    else
        # Last resort: wc -c (slower as it reads the file)
        wc -c < "$file" | tr -d ' '
    fi
}

for fname in $(ls $DEST/subtiles); do
    fqpath="$DEST/subtiles/$fname"
    size=$(get_file_size $fqpath)
    if [ "$size" -eq 0 ] && [ ! -d "$fqpath" ]; then
        echo "$fqpath was zero bytes. Maybe the copy operation failed?"
        exit 1
    fi
done;


echo "done."