# assumes SOURCE looks like
# .../bladeseq-2025.02.07-16.47.17/
# DEST looks like
# /some/directory/

# dependencies:
# tile transcoder: https://github.com/seung-lab/tile-transcoder
#  (install Pillow and imagecodecs too)
# GNU Parallel

# bladetranscode /mnt/sink/scratch/yannan/.../bladeseq-2025.02.07-16.47.17/ /mnt/sink/scratch/yannan/.../jxl/

SOURCE=$1
DEST=$2

BSEQ=$(basename $SOURCE)
SECTION=$(ls $SOURCE)
SOURCE="$SOURCE/$SECTION"

DEST="$DEST/$BSEQ/$SECTION"

mkdir -p $DEST

DBNAME=$(mktmp).db

NCPU=20

cp $SOURCE/*.jpg $DEST/
cp -r $SOURCE/metadata $DEST/
cp -r $SOURCE/montage $DEST/
cp -r $SOURCE/subtiles/metadata $DEST/subtiles/

transcode init "$SOURCE/subtiles" "$DEST/subtiles" --db $DBNAME --ext bmp --encoding jxl --compression none --jxl-effort 3 --jxl-decodingspeed 0
parallel -j $NCPU -N0 "transcode worker -b 1 $DBNAME" :: $(seq $NCPU)
rm $DBNAME

bmp_files=$(find "$SOURCE/subtiles" -type f -name '*.bmp' -printf '%f\n' | sed 's/\.bmp$//' | sort)
jxl_files=$(find "$DEST/subtiles" -type f -name '*.jxl' -printf '%f\n' | sed 's/\.jxl$//' | sort)

# Compare the file sets
diff_result=$(diff <(echo "$bmp_files") <(echo "$jxl_files"))

if [ -n "$diff_result" ]; then
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
	if [[ $size -eq 0 ]]; then
		echo "$fqpath was zero bytes. Maybe the copy operation failed?"
		exit 1
	fi
done;

echo "done."





