# Layout of files

This is probably a bit too rigid, but it works for us at the moment. If we plan to scale things up it may be worth properly thinking things through.

- slide1
  - scene1
    - tiles
      - round1
        - 0.ome.tiff
        - ...
      - round2
        - 0.ome.tiff
    - image.ome.tiff
    - image.ome.zarr
  - scene2
    - ...

  - .brightfield
  - .illumination
    - flatfield-{round}.tiff 
    - darkfield-{round}.tiff
  - .scan-logs
  - .logs
    - nextflow
      - before-annotate.log
      - before-annotate.report.html
      - before-annotate.trace.txt
      - before-annotate.timeline.html
      - after-annotate.log
      - after-annotate.report.html
      - after-annotate.trace.txt
      - after-annotate.timeline.html
    - illum-profiles
      - make-illum-profiles-{round}.dask-performance.html
      - plot-illum-profiles-{round}.svg
    - stack-tiles
      - stack-tiles-{round}-{scene}.dask-performance.html
    - unmixing-mosaics
      - make-unmixing-mosaic-{round}.dask-performance.html
    - segmentation
      - segment-{scene}.dask-performance.html

- calibration-dir
  - cube-alignment
    - gifs
    - images
    - plots
    - scales-shifts
    - .logs
      - cube-alignment-{timestamp}.dask-performance.html
  - dark
  - lamp
  - stage-xy

# TODO
- consider adding nextflow option to cache certain outputs, like illumination profiles

# Nextflow notes
* for conda to specify certain functions as entry points, the filenames of the files containing said functions must be importable (e.g. no dashes in filename)
