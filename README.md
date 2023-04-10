# `broadside-cycif-stitch`

*Broadside* is an image processing pipeline used by the Laboratory for Bio-Micro Devices @ Brigham & Women's Hospital in Boston, MA. As our image acquisition is a little different, some pre-processing steps are needed.

- microscope-specific calibration
  1. images under as little light as possible, to capture hot pixels

- cached adjustments
  1. for each slide, make flatfield and darkfield profiles per channel, using [pybasic](https://github.com/biomicrodev/pybasic) 

steps:
1. for each microscope, generate tables for correcting axial chromatic aberration
2. for each scene, make a stack of all tiles, applying the following functions
   1. remove hot pixels
   2. apply flatfield and darkfield corrections, per channel
   3. apply chromatic aberration corrections, per channel
3. for each stack of tiles in scene, apply [ASHLAR](https://github.com/labsyspharm/ashlar)

# Installation

Installation is not quite as streamlined as I'd like it to be.

Download the following:

- `git`, for cloning this repo
- `nextflow` (into somewhere on the PATH), for executing the nextflow pipelines
- `java`, for compiling java code; at least 11 (SDKMAN is recommended)
- `maven` (if on ubuntu, apt is good enough)
- `conda`, for executing the python code

1. clone this repository to a folder
2. copy the `example-nextflow.config` file and rename it to `nextflow.config`
   1. specify paths to the nextflow work directory, conda environment.yml file, and the calibration directory of the microscope that generated the tiles
3. navigate to `./nextflow` and run `mvn package`
4. run `bin/check-slide-status /path/to/slide/` to get some info on a slide
5. modify and run `bin/cycif-stitch /path/to/slide/` to run the pipeline for a slide
