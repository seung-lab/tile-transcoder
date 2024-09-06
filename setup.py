import os
import setuptools
import sys


PNG_DEPS = [ "pyspng-seunglab" ]
BMP_DEPS = [ "Pillow" ]
TIFF_DEPS = [ "tifffile" ]
JPEG_DEPS = [ "simplejpeg" ]

setuptools.setup(
  setup_requires=['pbr'],
  python_requires=">=3.8,<4.0",
  extras_require={
    "png": PNG_DEPS,
    "bmp": BMP_DEPS,
    "jpeg": JPEG_DEPS,
    "tiff": TIFF_DEPS,
    "all": PNG_DEPS + BMP_DEPS + TIFF_DEPS + JPEG_DEPS,
  },
  include_package_data=True,
  entry_points={
    "console_scripts": [
      "transcode=transcoder:cli_main"
    ],
  },
  pbr=True
)

