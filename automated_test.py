import pytest
import os
from cloudfiles import CloudFile
import zipfile
import subprocess
import shutil

import time

if not os.path.exists("./test_data/"):
    if not os.path.exists("./tile_transcoder_test_data.zip"):
        Cloudfile("./tile_transcoder_test_data.zip").transfer_from(
            "gs://seunglab/wms/tile_transcoder_test_data.zip"
        )

    with zipfile.ZipFile("./tile_transcoder_test_data.zip", 'r') as zip_ref:
        zip_ref.extractall("test_data")

DATA_PATH = "./test_data/bladeseq-2023.03.30-15.37.47/s013-2023.03.30-15.37.47/subtiles/"
DEST_PATH = "./test_data/jxl/"

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
