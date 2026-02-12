import pytest
import os
from cloudfiles import CloudFile
import zipfile
import subprocess
import shutil
import time
import simplejpeg
import numpy as np
import pyspng
import transcoder.encoding
import transcoder.detectors

if not os.path.exists("./test_data/"):
    if not os.path.exists("./tile_transcoder_test_data.zip"):
        Cloudfile("./tile_transcoder_test_data.zip").transfer_from(
            "gs://seunglab/wms/tile_transcoder_test_data.zip"
        )

    with zipfile.ZipFile("./tile_transcoder_test_data.zip", 'r') as zip_ref:
        zip_ref.extractall(".")
        shutil.rmtree("__MACOSX")

DATA_PATH = os.path.abspath("./test_data/bladeseq-2023.03.30-15.37.47/s013-2023.03.30-15.37.47/subtiles/")
DEST_PATH = os.path.abspath("./test_data/dest/")

BLACK_TILES = [
    'tile_0000_4.png',
    'tile_0000_5.png',
    'tile_0000_6.png',
    'tile_0000_7.png',
]

RESIN_TILES = [
    'tile_1000_1.png',
    'tile_1000_2.png',
    'tile_1000_4.png',
]

NON_TISSUE_TILES = RESIN_TILES + BLACK_TILES

def test_png_to_jxl():

    DB_PATH = "./auto_test_xfer.db"

    try:
        os.remove(DB_PATH)
    except FileNotFoundError:
        pass

    try:
        shutil.rmtree(DEST_PATH)
    except FileNotFoundError:
        pass

    init_cmd = f"transcode init {DATA_PATH} {DEST_PATH} --encoding jxl --level 100 --jxl-effort 1 --db {DB_PATH} --ext png"    
    subprocess.run(init_cmd, shell=True)
    subprocess.run(f"transcode worker -p 2 {DB_PATH} -b 2 --verbose --lease-msec 5000 --codec-threads 0 --cleanup", shell=True)

    srcfiles = os.listdir(DATA_PATH)
    srcfiles = [ x for x in srcfiles if '.png' in x ]
    destfiles = os.listdir(DEST_PATH)
    assert len(srcfiles) == len(destfiles)

    srcfiles = [ x.removesuffix(".png") for x in srcfiles ]
    destfiles = [ x.removesuffix(".jxl") for x in destfiles ]
    srcfiles.sort()
    destfiles.sort()

    assert srcfiles == destfiles

@pytest.mark.parametrize("encoding", [ "jpeg", "jpegxl", "bmp", "tiff", "png",  ])
def test_transcoder_function(encoding):

    filename = os.listdir(DATA_PATH)[0]
    with open(os.path.join(DATA_PATH, filename), "rb") as f:
        binary = f.read()

    img = transcoder.encoding.decode(binary, encoding="png")

    (new_filename, new_binary) = transcoder.encoding.transcode_image(
        filename, binary, encoding=encoding, level=100,
    )

    assert len(new_binary) > 0

    recovered_img = transcoder.encoding.decode(new_binary, encoding=encoding)

    if encoding != "jpeg":
        assert np.all(img == recovered_img)
    else:
        assert np.abs(np.max(img.astype(int) - recovered_img.astype(int))) < 3
        assert np.abs(np.mean(img) - np.mean(recovered_img)) < 3


def test_tissue_detector():
    for filename in os.listdir(DATA_PATH):
        if '.png' not in filename:
            continue
        with open(os.path.join(DATA_PATH, filename), "rb") as f:
            binary = f.read()

        img = transcoder.encoding.decode(binary, encoding="png")
        has_tissue = transcoder.detectors.tem_subtile_has_tissue(img)
        gt_has_tissue = not (filename in NON_TISSUE_TILES)
        assert has_tissue == gt_has_tissue 

    black = np.zeros([6000,6000], dtype=np.uint8)
    assert not transcoder.detectors.tem_subtile_has_tissue(black)

    bright = np.zeros([6000,6000], dtype=np.uint8) + 186
    assert not transcoder.detectors.tem_subtile_has_tissue(bright)

def test_resin_stay_handling():

    DB_PATH = "./auto_test_xfer.db"

    try:
        os.remove(DB_PATH)
    except FileNotFoundError:
        pass

    try:
        shutil.rmtree(DEST_PATH)
    except FileNotFoundError:
        pass

    init_cmd = f"transcode init {DATA_PATH} {DEST_PATH} --encoding jpeg --level 100 --db {DB_PATH} --ext png --resin stay"    
    subprocess.run(init_cmd, shell=True)
    subprocess.run(f"transcode worker -p 2 {DB_PATH} -b 2 --verbose --lease-msec 5000 --codec-threads 0 --cleanup", shell=True)

    srcfiles = os.listdir(DATA_PATH)
    srcfiles = [ x for x in srcfiles if '.png' in x ]
    destfiles = os.listdir(DEST_PATH)

    srcfiles = set(srcfiles)
    srcfiles -= set(NON_TISSUE_TILES)

    srcfiles = sorted(list(srcfiles))
    destfiles = sorted(destfiles)

    srcfiles = [ x.removesuffix(".png") for x in srcfiles ]
    destfiles = [ x.removesuffix(".jpeg") for x in destfiles ]

    assert srcfiles == destfiles

    assert os.path.exists(os.path.join(os.path.dirname(DATA_PATH), 'logs'))
    shutil.rmtree(os.path.join(os.path.dirname(DATA_PATH), 'logs'))

def test_resin_move_handling():

    DB_PATH = "./auto_test_xfer.db"

    try:
        os.remove(DB_PATH)
    except FileNotFoundError:
        pass

    try:
        shutil.rmtree(DEST_PATH)
    except FileNotFoundError:
        pass

    init_cmd = f"transcode init {DATA_PATH} {DEST_PATH} --encoding jpeg --level 100 --db {DB_PATH} --ext png --resin move"    
    subprocess.run(init_cmd, shell=True)
    subprocess.run(f"transcode worker {DB_PATH} -b 1 --verbose --lease-msec 5000 --codec-threads 0 --cleanup", shell=True)

    srcfiles = os.listdir(DATA_PATH)
    srcfiles = [ x for x in srcfiles if '.png' in x ]
    destfiles = os.listdir(DEST_PATH)

    srcfiles = set(srcfiles)
    srcfiles = sorted(list(srcfiles))
    destfiles = sorted(destfiles)

    srcfiles = [ x.removesuffix(".png") for x in srcfiles ]
    destfiles = [ x.removesuffix(".jpeg") for x in destfiles ]

    assert srcfiles == destfiles

    resin_path = os.path.join(os.path.dirname(DATA_PATH), 'resin')
    assert os.path.exists(resin_path)
    assert set(os.listdir(resin_path)) == set(NON_TISSUE_TILES)

    for filename in os.listdir(resin_path):
        fullpath = os.path.join(resin_path, filename)
        shutil.move(fullpath, os.path.join(DATA_PATH, filename))

    shutil.rmtree(os.path.join(os.path.dirname(DATA_PATH), 'logs'))

def setup_test_source(test_name):
    """Create a copy of the test data for a specific test to avoid interference."""
    source_copy = os.path.abspath(f"./test_data/source_copy_{test_name}/")
    
    try:
        shutil.rmtree(source_copy)
    except FileNotFoundError:
        pass
    
    os.makedirs(source_copy, exist_ok=True)
    
    # Copy files from original data path
    for filename in os.listdir(DATA_PATH):
        if filename.endswith('.png'):
            shutil.copy(
                os.path.join(DATA_PATH, filename),
                os.path.join(source_copy, filename)
            )
    
    return source_copy

def cleanup_paths(*paths):
    """Clean up test directories and databases."""
    for path in paths:
        if os.path.isfile(path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        elif os.path.isdir(path):
            try:
                shutil.rmtree(path)
            except FileNotFoundError:
                pass


def test_delete_original_basic():
    """Test that --delete-original flag removes source files after transcoding."""
    
    DB_PATH = "./test_delete_original_basic.db"
    source_path = setup_test_source("basic")
    dest_path = os.path.abspath("./test_data/dest_delete_basic/")
    
    cleanup_paths(DB_PATH, dest_path)
    
    # Get list of original files
    original_files = set(os.listdir(source_path))
    original_count = len([f for f in original_files if f.endswith('.png')])
    
    assert original_count > 0, "No test files found"
    
    # Initialize with delete-original flag
    init_cmd = (
        f"transcode init {source_path} {dest_path} "
        f"--encoding jpeg --level 95 --db {DB_PATH} "
        f"--ext png --delete-original"
    )
    subprocess.run(init_cmd, shell=True, check=True)
    
    # Run worker
    worker_cmd = (
        f"transcode worker {DB_PATH} -b 2 --verbose "
        f"--lease-msec 5000 --codec-threads 0"
    )
    subprocess.run(worker_cmd, shell=True, check=True)
    
    # Check that destination files exist
    dest_files = os.listdir(dest_path)
    assert len(dest_files) == original_count, "Not all files were transcoded"
    
    # Check that source files were deleted
    remaining_files = [f for f in os.listdir(source_path) if f.endswith('.png')]
    assert len(remaining_files) == 0, f"Source files were not deleted: {remaining_files}"
    
    # Verify transcoded files are valid
    for dest_file in dest_files:
        dest_filepath = os.path.join(dest_path, dest_file)
        with open(dest_filepath, 'rb') as f:
            binary = f.read()
        
        # Should be able to decode as JPEG
        img = transcoder.encoding.decode(binary, encoding="jpeg")
        assert img is not None
        assert len(img.shape) == 2 or len(img.shape) == 3
    
    cleanup_paths(DB_PATH, source_path, dest_path)


def test_delete_original_without_flag():
    """Test that source files are NOT deleted when --delete-original is not set."""
    
    DB_PATH = "./test_delete_original_without_flag.db"
    source_path = setup_test_source("without_flag")
    dest_path = os.path.abspath("./test_data/dest_no_delete/")
    
    cleanup_paths(DB_PATH, dest_path)
    
    # Get list of original files
    original_files = set(os.listdir(source_path))
    original_count = len([f for f in original_files if f.endswith('.png')])
    
    # Initialize WITHOUT delete-original flag
    init_cmd = (
        f"transcode init {source_path} {dest_path} "
        f"--encoding jpeg --level 95 --db {DB_PATH} --ext png"
    )
    subprocess.run(init_cmd, shell=True, check=True)
    
    # Run worker
    worker_cmd = f"transcode worker {DB_PATH} -b 2 --lease-msec 5000"
    subprocess.run(worker_cmd, shell=True, check=True)
    
    # Check that destination files exist
    dest_files = os.listdir(dest_path)
    assert len(dest_files) == original_count
    
    # Check that source files still exist
    remaining_files = [f for f in os.listdir(source_path) if f.endswith('.png')]
    assert len(remaining_files) == original_count, "Source files were deleted when they shouldn't be"
    
    cleanup_paths(DB_PATH, source_path, dest_path)

@pytest.mark.parametrize("encoding", ["png", "jpeg", "jpegxl", "bmp", "tiff"])
def test_delete_original_with_different_encodings(encoding):
    """Test --delete-original works with various encoding formats."""
    
    DB_PATH = f"./test_delete_encoding_{encoding}.db"
    source_path = setup_test_source(f"encoding_{encoding}")
    dest_path = os.path.abspath(f"./test_data/dest_delete_{encoding}/")
    
    cleanup_paths(DB_PATH, dest_path)
    
    original_count = len([f for f in os.listdir(source_path) if f.endswith('.png')])
    
    # Initialize with delete-original and specific encoding
    init_cmd = (
        f"transcode init {source_path} {dest_path} "
        f"--encoding {encoding} --level 100 --db {DB_PATH} "
        f"--ext png --delete-original"
    )
    
    if encoding == "jpegxl":
        init_cmd += " --jxl-effort 1"
    
    subprocess.run(init_cmd, shell=True, check=True)
    
    # Run worker
    worker_cmd = f"transcode worker {DB_PATH} -b 2 --lease-msec 5000 --codec-threads 0"
    subprocess.run(worker_cmd, shell=True, check=True)
    
    # Verify destination files exist
    dest_files = os.listdir(dest_path)
    assert len(dest_files) == original_count, f"Failed for {encoding}"
    
    # Verify source files deleted
    remaining = [f for f in os.listdir(source_path) if f.endswith('.png')]
    assert len(remaining) == 0, f"Source files not deleted for {encoding}: {remaining}"
    
    cleanup_paths(DB_PATH, source_path, dest_path)

def test_delete_original_with_resin_stay():
    """Test --delete-original interacts correctly with --resin stay."""
    
    DB_PATH = "./test_delete_resin_stay.db"
    source_path = setup_test_source("resin_stay")
    dest_path = os.path.abspath("./test_data/dest_resin_stay/")
    
    cleanup_paths(DB_PATH, dest_path)
    
    original_files = [f for f in os.listdir(source_path) if f.endswith('.png')]
    original_count = len(original_files)
    expected_transcoded = original_count - len(NON_TISSUE_TILES)
    
    # Initialize with both delete-original and resin stay
    init_cmd = (
        f"transcode init {source_path} {dest_path} "
        f"--encoding jpeg --level 95 --db {DB_PATH} "
        f"--ext png --delete-original --resin stay"
    )
    subprocess.run(init_cmd, shell=True, check=True)
    
    # Run worker
    worker_cmd = f"transcode worker {DB_PATH} -b 2 --lease-msec 5000 --cleanup"
    subprocess.run(worker_cmd, shell=True, check=True)
    
    # Check destination has non-resin files
    dest_files = os.listdir(dest_path)
    assert len(dest_files) == expected_transcoded, (
        f"Expected {expected_transcoded} transcoded files, got {len(dest_files)}"
    )
    
    # Check source: resin tiles should remain, others should be deleted
    remaining_files = set(os.listdir(source_path))
    remaining_png = {f for f in remaining_files if f.endswith('.png')}
    
    assert remaining_png == set(NON_TISSUE_TILES), (
        f"Expected only resin tiles to remain, but got: {remaining_png}"
    )
    
    # Clean up logs directory
    logs_path = os.path.join(os.path.dirname(source_path), 'logs')
    if os.path.exists(logs_path):
        shutil.rmtree(logs_path)
    
    cleanup_paths(DB_PATH, source_path, dest_path)

def test_delete_original_in_place():
    """Test --delete-original when source and destination are the same (in-place transcoding)."""
    
    DB_PATH = "./test_delete_in_place.db"
    source_path = setup_test_source("in_place")
    
    cleanup_paths(DB_PATH)
    
    original_files = [f for f in os.listdir(source_path) if f.endswith('.png')]
    original_count = len(original_files)
    
    # Initialize with same source and destination (in-place) and delete-original
    init_cmd = (
        f"transcode init {source_path} {source_path} "
        f"--encoding jpeg --level 95 --db {DB_PATH} "
        f"--ext png --delete-original"
    )
    subprocess.run(init_cmd, shell=True, check=True)
    
    # Run worker
    worker_cmd = f"transcode worker {DB_PATH} -b 2 --lease-msec 5000"
    subprocess.run(worker_cmd, shell=True, check=True)
    
    # Check that JPEG files exist
    jpeg_files = [f for f in os.listdir(source_path) if f.endswith('.jpeg')]
    assert len(jpeg_files) == original_count, "Not all files transcoded to JPEG"
    
    # Check that original PNG files were deleted
    png_files = [f for f in os.listdir(source_path) if f.endswith('.png')]
    assert len(png_files) == 0, f"Original PNG files not deleted: {png_files}"
    
    cleanup_paths(DB_PATH, source_path)

