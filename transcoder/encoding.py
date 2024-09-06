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

def check_installed(encoding):
  if encoding in NEEDS_INSTALL:
    raise ImportError(f"Optional codec {encoding} is not installed. Run: pip install {NEEDS_INSTALL[encoding]}")

def transcode_image(filename:str, binary:bytes, encoding:str) -> bytes:
  basename, ext = os.path.splitext(filename)

  src_encoding = ext[1:] # eliminate '.'
  src_encoding = src_encoding.lower()

  if src_encoding == encoding:
    return binary

  img = decode(binary, src_encoding)
  ext, binary = encode(img, encoding)
  return (basename + ext, binary)

def decode(binary:bytes, encoding:str) -> bytes:

  check_installed(encoding)

  if encoding == "bmp":
    return bmp_to_npy(binary)
  elif encoding == "png":
    return pyspng.load(binary)
  else:
    raise EncodingNotSupported(f"{encoding}")

def encode(img:np.ndarray, encoding:str) -> Tuple[str, bytes]:

  check_installed(encoding)

  if encoding == "png":
    return (".png", pyspng.encode(img, compress_level=8))
  elif encoding == "bmp":
    return (".bmp", npy_to_bmp(img))
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



  


