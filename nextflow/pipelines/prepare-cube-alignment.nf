import groovy.io.FileType

import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Matcher
import java.util.regex.Pattern

// cube alignment-specific parameters
params.overwriteCubeAlignments = false

class CubeAlignPaths {
    /*
    a place to collect paths containing bits of data relative to the calibration path:
    - calibration
      - cube-alignment
        - gifs
        - images
        - plots
        - scales-shifts
    We kinda need something global like this because these paths are all absolute
     */
    Path calibrationDir
    Path alignDir = calibrationDir.resolve("cube-alignment")
    Path imagesDir = alignDir.resolve("images")
    Path gifsDir = alignDir.resolve("gifs")
    Path scalesShiftsDir = alignDir.resolve("scales-shifts")
    Path plotsDir = alignDir.resolve("plots")
    Path logDir = alignDir.resolve(".logs")

    CubeAlignPaths(Path calibrationDir) {
        this.calibrationDir = calibrationDir
    }
}

cubeAlignPaths = new CubeAlignPaths(Paths.get(params.calibrationDir))

def getCubeStackTimestamps() {
    /*
    What is this supposed to do?
    In the images folder containing acquired cube stack pairs, get the integer-type
    unix timestamps within the filenames
     */
    Pattern cubeStackPattern = ~/cube-stack-(?<timestamp>[0-9]+)-im[01]\.ome\.tiff/

    def timestamps = [] as Set
    cubeAlignPaths.imagesDir.toFile().eachFile(FileType.FILES) {
        String filename = it.toPath().getFileName().toString()
        Matcher match = filename =~ cubeStackPattern
        if (match.matches()) {
            timestamps.add(match.group("timestamp") as int)
        }
    }
    return timestamps
}

def doComputeCubeAlignments(int timestamp) {
    /*
    What is this supposed to do?
    Return if not all of the generated files exist.
     */
    boolean csvExists = cubeAlignPaths.scalesShiftsDir.resolve("${timestamp}.csv").toFile().exists()
    boolean gifExists = cubeAlignPaths.gifsDir.resolve("${timestamp}.gif").toFile().exists()
    boolean plotExists = cubeAlignPaths.plotsDir.resolve("${timestamp}.svg").toFile().exists()

    return !(csvExists && gifExists && plotExists)
}

process COMPUTE_CUBE_ALIGNMENTS {
    if (!workflow.stubRun) {
        conda "${params.condaEnvironmentPath}"
    }
    tag "timestamp: ${timestamp}"
    memory "32 GB"
    cpus 4

    publishDir(
        path: "${cubeAlignPaths.scalesShiftsDir}",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "*.csv",
    )
    publishDir(
        path: "${cubeAlignPaths.gifsDir}",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "*.gif",
    )
    publishDir(
        path: "${cubeAlignPaths.plotsDir}",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "*.svg",
    )
    publishDir(
        path: "${cubeAlignPaths.logDir}",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "cube-alignment-*.dask-performance.html",
    )

    input:
        val timestamp

    output:
        val timestamp, emit: timestamps
        path "${timestamp}.csv"
        path "${timestamp}.gif"
        path "${timestamp}.svg"
        path "cube-alignment-${timestamp}.dask-performance.html"

    script:
        """
        compute-cube-alignments \
            --ref-path "${cubeAlignPaths.imagesDir}/cube-stack-${timestamp}-im0.ome.tiff" \
            --mov-path "${cubeAlignPaths.imagesDir}/cube-stack-${timestamp}-im1.ome.tiff" \
            --csv-path "${timestamp}.csv" \
            --gif-path "${timestamp}.gif" \
            --plot-path "${timestamp}.svg" \
            --timestamp "${timestamp}" \
            --n-cpus "${task.cpus}" \
            --memory-limit "${task.memory}" \
            --dask-report-filename "cube-alignment-${timestamp}.dask-performance.html"
        """

    stub:
        """
        touch "${timestamp}.csv"
        touch "${timestamp}.gif"
        touch "${timestamp}.svg"
        touch "cube-alignment-${timestamp}.dask-performance.html"
        """
}

workflow PREPARE_CUBE_ALIGNMENTS {
    main:
        channel.fromList(getCubeStackTimestamps()).set { timestampsCh }
        if (!params.overwriteCubeAlignments) {
            timestampsCh
                .filter { doComputeCubeAlignments(it) }
                .set { timestampsCh }
        }
        COMPUTE_CUBE_ALIGNMENTS(timestampsCh)

    emit:
        timestamps = COMPUTE_CUBE_ALIGNMENTS.out.timestamps
}