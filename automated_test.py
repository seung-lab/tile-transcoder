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
        zip_ref.extractall("test_data")

DATA_PATH = os.path.abspath("./test_data/bladeseq-2023.03.30-15.37.47/s013-2023.03.30-15.37.47/subtiles/")
DEST_PATH = os.path.abspath("./test_data/dest/")

RESIN_TILES = [
    'tile_1000_1.png',
    'tile_1000_2.png',
    'tile_1000_4.png',
]

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
        gt_has_tissue = not (filename in RESIN_TILES)
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
    srcfiles -= set(RESIN_TILES)

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
    assert set(os.listdir(resin_path)) == set(RESIN_TILES)

    for filename in os.listdir(resin_path):
        fullpath = os.path.join(resin_path, filename)
        shutil.move(fullpath, os.path.join(DATA_PATH, filename))

    shutil.rmtree(os.path.join(os.path.dirname(DATA_PATH), 'logs'))





