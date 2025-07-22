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
import numpy.typing as npt

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

class EncodingNotSupported(Exception):
  pass

def transcode_image(
  filename:str, 
  binary:bytes, 
  encoding:str, 
  level:Optional[int],
  callback:Optional[Callable[[npt.NDArray[np.uint8]], bool]] = None,
  **kwargs,
) -> tuple[str, bytes]:
  """
  Transcodes a given image to a new image type
  as efficiently as possible.

  Under some circumstances, such as matching source and destination
  encoding or jpeg<->jxl, we can avoid decoding the image.

  filename: file path with the current file extension (e.g. image.png)
    the file extension is how it knows the source encoding.
  binary: the encoded file as byte string
  encoding: the destination encoding (e.g. png, jxl, etc)
  level: quality level
  callback: 
    If provided, forces decoding of the image and passes it to
    this as callable(path, img). The bool return type tells whether
    to continue with the transcoding process. For example,
    a given callable might move a file instead of transcoding it
    based on a computer vision result and return False meaning,
    don't continue wasting time with encoding.

  Returns (filename + ext, transcoded binary)
  """
  basename, ext = os.path.splitext(filename)

  src_encoding = ext[1:] # eliminate '.'
  src_encoding = src_encoding.lower()
  encoding = encoding.lower()

  num_threads = kwargs.get("num_threads", None)

  decoded_image = None 
  if callback:
    try:
      decoded_image = decode(binary, src_encoding, num_threads=num_threads)
    except:
      print(f"Decoding Error: {filename}", file=sys.stderr)
      raise

    if not callback(filename, decoded_image):
      return (filename, binary)

  if src_encoding == encoding:
    return (filename, binary)
  elif src_encoding == "jpeg" and encoding in ["jpegxl", "jxl"] and level is None:
    return (basename + ".jxl", jpegxl_encode_jpeg(binary, numthreads=num_threads))
  elif src_encoding in ["jpegxl", "jxl"] and encoding == "jpeg" and level is None:
    return (basename + ".jpeg", jpegxl_decode_jpeg(binary, numthreads=num_threads))
  else:
    if decoded_image is None:
      try:
        img = decode(binary, src_encoding, num_threads=num_threads)
      except:
        print(f"Decoding Error: {filename}", file=sys.stderr)
        raise
    else:
      img = decoded_image

    try:
      ext, binary = encode(img, encoding, level, **kwargs)
    except:
      print(f"Encoding Error: {filename}", file=sys.stderr)
      raise

    return (basename + ext, binary)

def decode(binary:bytes, encoding:str, num_threads:Optional[int] = None) -> np.ndarray:

  check_installed(encoding)

  if encoding == "bmp":
    return bmp_to_npy(binary)
  elif encoding == "png":
    return pyspng.load(binary)
  elif encoding == "jpeg":
    return simplejpeg.decode_jpeg(binary)
  elif encoding in ["jpegxl", "jxl"]:
    return imagecodecs.jpegxl_decode(binary, numthreads=num_threads)
  elif encoding in ["tiff", "tif"]:
    return tiff_to_npy(binary)
  else:
    raise EncodingNotSupported(f"{encoding}")

def encode(
  img:np.ndarray, 
  encoding:str, 
  level:Optional[int] = None,
  num_threads:Optional[int] = None,
  **kwargs,
) -> Tuple[str, bytes]:

  check_installed(encoding)

  if encoding == "png":
    return (".png", pyspng.encode(img, compress_level=8))
  elif encoding == "bmp":
    return (".bmp", npy_to_bmp(img))
  elif encoding == "jpeg":
    return (".jpeg", encode_jpeg(img, level))
  elif encoding in ["jpegxl", "jxl"]:
    return (".jxl", encode_jpegxl(
      img, level, 
      effort=int(kwargs.get("effort", 3)),
      decodingspeed=int(kwargs.get("decodingspeed", 0)),
      numthreads=num_threads,
    ))
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

def encode_jpegxl(arr, level, effort, decodingspeed, numthreads):
  if not np.issubdtype(arr.dtype, np.uint8):
    raise ValueError(f"Only accepts uint8 arrays. Got: {arr.dtype}")

  num_channel = 1
  if len(arr.shape) > 2:
    num_channel = arr.shape[2]

  if level is None:
    level = 90 # visually lossless

  lossless = level >= 100

  if num_channel != 1:
    raise ValueError(f"Number of image channels should be 1. 3 possible, but not implemented. Got: {arr.shape[3]}")

  while len(arr.shape) > 2:
    arr = arr[...,0]

  return imagecodecs.jpegxl_encode(
    arr[:,:],
    photometric="GRAY",
    level=level,
    lossless=lossless,
    effort=effort,
    decodingspeed=decodingspeed,
    numthreads=numthreads,
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

