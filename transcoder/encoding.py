from typing import (
  Any, Dict, Optional, 
  Union, List, Tuple, 
  Callable, Generator, 
  Iterable, cast
)

import io
import os
import sys

import numpy as np

NEEDS_INSTALL = {}

try:
  import pyspng
except ImportError:
  NEEDS_INSTALL["png"] = "pyspng-seunglab"

try:
  from PIL import Image
except ImportError:
  NEEDS_INSTALL["bmp"] = "Pillow"

try:
  import simplejpeg
except ImportError:
  NEEDS_INSTALL["jpeg"] = "simplejpeg"

try:
  import imagecodecs
  from imagecodecs import jpegxl_encode_jpeg, jpegxl_decode_jpeg
except ImportError:
  NEEDS_INSTALL["jxl"] = "imagecodecs"

try:
  import tifffile
except ImportError:
  NEEDS_INSTALL["tiff"] = "tifffile"

def check_installed(encoding):
  if encoding in NEEDS_INSTALL:
    raise ImportError(f"Optional codec {encoding} is not installed. Run: pip install {NEEDS_INSTALL[encoding]}")

def transcode_image(
  filename:str, 
  binary:bytes, 
  encoding:str, 
  level:Optional[int],
) -> bytes:
  basename, ext = os.path.splitext(filename)

  src_encoding = ext[1:] # eliminate '.'
  src_encoding = src_encoding.lower()
  encoding = encoding.lower()

  if src_encoding == encoding:
    return binary

  elif src_encoding == "jpeg" and encoding in ["jpegxl", "jxl"] and level is None:
    return (basename + ".jxl", jpegxl_encode_jpeg(binary))
  elif src_encoding in ["jpegxl", "jxl"] and encoding == "jpeg" and level is None:
    return (basename + ".jpeg", jpegxl_decode_jpeg(binary))
  else:
    try:
      img = decode(binary, src_encoding)
    except:
      print(f"Decoding Error: {filename}", file=sys.stderr)
      raise

    try:
      ext, binary = encode(img, encoding, level)
    except:
      print(f"Encoding Error: {filename}", file=sys.stderr)
      raise

    return (basename + ext, binary)

def decode(binary:bytes, encoding:str) -> np.ndarray:

  check_installed(encoding)

  if encoding == "bmp":
    return bmp_to_npy(binary)
  elif encoding == "png":
    return pyspng.load(binary)
  elif encoding == "jpeg":
    return simplejpeg.decode_jpeg(binary)
  elif encoding in ["jpegxl", "jxl"]:
    return imagecodecs.jpegxl_decode(binary)
  elif encoding in ["tiff", "tif"]:
    return tiff_to_npy(binary)
  else:
    raise EncodingNotSupported(f"{encoding}")

def encode(img:np.ndarray, encoding:str, level:Optional[int]) -> Tuple[str, bytes]:

  check_installed(encoding)

  if encoding == "png":
    return (".png", pyspng.encode(img, compress_level=8))
  elif encoding == "bmp":
    return (".bmp", npy_to_bmp(img))
  elif encoding == "jpeg":
    return (".jpeg", encode_jpeg(img, level))
  elif encoding in ["jpegxl", "jxl"]:
    return (".jxl", encode_jpegxl(img, level))
  elif encoding in ["tiff", "tif"]:
    return npy_to_tiff(img)
  else:
    raise EncodingNotSupported(f"{encoding}")

def bmp_to_npy(binary:bytes) -> np.ndarray:
  img = Image.open(io.BytesIO(binary))
  return np.array(img)

def npy_to_bmp(img:np.ndarray) -> bytes:
  buf = io.BytesIO()
  Image.fromarray(img).save(buf, format="BMP")
  buf.seek(0)
  return buf.read()

def tiff_to_npy(binary:bytes) -> np.ndarray:
  buf = io.BytesIO(binary)
  return tifffile.imread(buf)

def npy_to_tiff(img:np.ndarray) -> bytes:
  buf = io.BytesIO()
  tifffile.imwrite(buf, img, photometric='minisblack')
  buf.seek(0)
  return buf.read()

def encode_jpegxl(arr, level):
  if not np.issubdtype(arr.dtype, np.uint8):
    raise ValueError(f"Only accepts uint8 arrays. Got: {arr.dtype}")

  num_channel = arr.shape[2]
  lossless = level >= 100

  if level is None:
    level = 85

  if num_channel != 1:
    raise ValueError(f"Number of image channels should be 1. 3 possible, but not implemented. Got: {arr.shape[3]}")

  return imagecodecs.jpegxl_encode(
    arr[:,:,0],
    photometric="GRAY",
    level=level,
    lossless=lossless,
  )

def encode_jpeg(arr, quality):
  if not np.issubdtype(arr.dtype, np.uint8):
    raise ValueError(f"Only accepts uint8 arrays. Got: {arr.dtype}")

  arr = np.ascontiguousarray(arr)

  if quality is None:
    quality = 85

  if num_channel == 1:
    return simplejpeg.encode_jpeg(
      arr, 
      colorspace="GRAY",
      colorsubsampling="GRAY",
      quality=quality,
    )
  elif num_channel == 3:
    return simplejpeg.encode_jpeg(
      arr,
      colorspace="RGB",
      quality=quality,
    )
  raise ValueError(f"Number of image channels should be 1 or 3. Got: {arr.shape[3]}")

