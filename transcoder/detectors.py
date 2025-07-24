from typing import Callable, Optional
import enum

import numpy as np
import numpy.typing as npt

from cloudfiles import CloudFiles

try:
  import cc3d

  import tinybrain

  import os
  import scipy.signal
  import cv2
  DETECTOR = True
except ImportError:
  DETECTOR = False
  pass

class ResinHandling(enum.IntEnum):
  NOOP = 0
  MOVE = 1
  DELETE = 2
  LOSSY = 3
  LOG = 4

def tem_subtile_has_tissue(img:npt.NDArray[np.uint8]) -> bool:
  """
  Checks if there is tissue in a Transmission Electron Microscopy 
  (TEM) image.
  
  The parameters were tuned for cricket TEM subtiles imaged on
  Luxel Tape EM in the Bezos Imaging Center of the Princeton
  Neuroscience Institute in New Jersey, USA.
  """
  if not DETECTOR:
    raise ImportError("You need to install tile transcoder with the 'detectors' optional dependencies.")

  ds = tinybrain.downsample_with_averaging(img, (2,2,1), num_mips=3)
  img = ds[-1]
  hist, bin_edges = np.histogram(img.ravel(), bins=20)
  peak_idxs, bounds = scipy.signal.find_peaks(hist, height=500, threshold=4000)

  if len(peak_idxs) != 1:
    return True

  mean = np.mean(img)
  if mean <= 185:
    return True

  stdev = np.std(img)
  if stdev >= 11:
    return True

  edges = cv2.Canny(img, threshold1=120, threshold2=200)
  edges = cc3d.dust(edges, threshold=35)

  return np.any(edges)


def make_resin_action(source:str, verbose:bool, resin_handling:int) -> Optional[Callable[[str, npt.NDArray[np.uint8]], None]]:
  cf_src = CloudFiles(source)

  resin_move_path = cf_src.join(source, "../resin/")

  def move_resin(path, img):
    if tem_subtile_has_tissue(img):
      return

    if verbose:
      print(f"No tissue detected. Moving {path} to {resin_move_path}")

    fullpath_src = cf_src.join(source, path)
    fullpath_dest = cf_src.join(resin_move_path, path)
    CloudFile(fullpath_src).move(fullpath_dest)
    raise SkipTranscoding()

  def log_resin(path, img):
    if tem_subtile_has_tissue(img):
      return

    if verbose:
      print(f"No tissue detected in {path}.")

  if resin_handling == ResinHandling.MOVE:
    return move_resin
  elif resin_handling == ResinHandling.LOG:
    return log_resin

  return None


