import java.util.Random
import java.nio.file.Path
import java.nio.file.Paths

// illumination-specific parameters
params.computeDarkfield = false
params.nMaxIllum = 200
params.nAssessIllum = 5
params.illumProfilesWorkingSize = 128

// local variables
Random randomSeed = new Random(0)
String newline = "\n"

process GET_RANDOM_ILLUM_TILES_BY_ROUND {
    tag "round: ${round}"

    input:
        val slide
        val round

    output:
        tuple \
            val(round), \
            path("random-illum-tile-paths-${round}.txt")

    exec:
        List<Path> tilePaths = slide.getTilePathsForRound(round)
        tilePaths.shuffle(randomSeed)
        int nTiles = Math.min(params.nMaxIllum, tilePaths.size()).toInteger()
        List<Path> selectedTilePaths = tilePaths[0..<nTiles]
        Path dst = task.workDir.resolve("random-illum-tile-paths-${round}.txt")
        file(dst).text = selectedTilePaths.join(newline)
}

process GET_ALL_ILLUM_TILES_BY_ROUND {
    tag "round: ${round}"

    input:
        val slide
        val round

    output:
        tuple \
            val(round), \
            path("all-illum-tile-paths-${round}.txt")

    exec:
        Path dst = task.workDir.resolve("all-illum-tile-paths-${round}.txt")
        file(dst).text = slide.getTilePathsForRound(round).join(newline)
}

process MAKE_ILLUM_PROFILES_BY_ROUND {
    /*
    This step has a parallel step, where images are read in, and a serial step, where
    the flatfield and darkfield images are computed iteratively. The memory usage of
    this step is dependent on the number of tiles, but because it is essentially fixed,
    we don't expect to change this. Obviously this may change in the future, so if
    anyone changes the number of tiles picked for this step, the memory will have to
    change as well.

    This step doesn't really need that much computing power; it is not really able to
    be run in parallel.
    */

    if (!workflow.stubRun) {
        conda "${params.condaEnvironmentPath}"
    }
    tag "round: ${round}"
    memory "2 GB"
    cpus 2

//     publishDir(
//         path: "${illumDir}",
//         enabled: !workflow.stubRun,
//         mode: "copy",
//         pattern: "flatfield-*.tiff",
//     )
//     publishDir(
//         path: "${illumDir}",
//         enabled: !workflow.stubRun,
//         mode: "copy",
//         pattern: "darkfield-*.tiff",
//     )
    // log dask performance reports
    publishDir(
        path: "${params.logDir}/illum-profiles/",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "make-illum-profiles-*.dask-performance.html",
    )

    input:
        tuple \
            val(round), \
            path(tilesPath)
//         val illumDir

    output:
        tuple \
            val(round), \
            path("flatfield-${round}.tiff"), \
            path("darkfield-${round}.tiff"), emit: profiles
        path "make-illum-profiles-${round}.dask-performance.html", optional: true

    script:
        Path darkDir = Paths.get(params.calibrationDir).resolve("dark")
        String darkfieldArg = params.computeDarkfield ? "--darkfield" : "--no-darkfield"
        """
        make-illum-profiles \
            --tiles-path "${tilesPath}" \
            --flatfield-path "flatfield-${round}.tiff" \
            --darkfield-path "darkfield-${round}.tiff" \
            "${darkfieldArg}" \
            --dark-dir "${darkDir}" \
            --working-size "${params.illumProfilesWorkingSize}" \
            --n-cpus "${task.cpus}" \
            --memory-limit "${task.memory}" \
            --dask-report-filename "make-illum-profiles-${round}.dask-performance.html"
        """

    stub:
        """
        touch "flatfield-${round}.tiff"
        touch "darkfield-${round}.tiff"
        touch "make-illum-profiles-${round}.dask-performance.html"
        """
}

process ASSESS_ILLUM_PROFILES_BY_ROUND {
    if (!workflow.stubRun) {
        conda "${params.condaEnvironmentPath}"
    }
    tag "round: ${round}"
    memory "2 GB"
    cpus 1

    publishDir(
        path: "${params.logDir}/illum-profiles/",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "plot-illum-profiles-*.svg",
    )

    input:
        tuple \
            val(round), \
            path(tilesPath), \
            path(flatfieldPath), \
            path(darkfieldPath)

    output:
        path "plot-illum-profiles-${round}.svg"

    script:
        Path darkDir = Paths.get(params.calibrationDir).resolve("dark")
        """
        assess-illum-profiles \
            --round-name "${round}" \
            --tiles-path "${tilesPath}" \
            --n-tiles "${params.nAssessIllum}" \
            --flatfield-path "${flatfieldPath}" \
            --darkfield-path "${darkfieldPath}" \
            --dark-dir "${darkDir}" \
            --dst "plot-illum-profiles-${round}.svg"
        """

    stub:
        """
        touch "plot-illum-profiles-${round}.svg"
        """
}

boolean doComputeIllumProfiles(Path illumDir, String round) {
    boolean flatfieldExists = illumDir.resolve("flatfield-${round}.tiff").toFile().exists()
    boolean darkfieldExists = illumDir.resolve("darkfield-${round}.tiff").toFile().exists()
    return !(flatfieldExists && darkfieldExists)
}

boolean doAssessIllumProfiles(Path illumProfilesDir, String round) {
    boolean illumAssessmentExists = illumProfilesDir.resolve("plot-illum-profiles-${round}.svg").toFile().exists()
    return !illumAssessmentExists
}

workflow PREPARE_ROUNDS {
    /*
    This module takes as argument a timestamps channel so that it runs after the cube
    alignment step.
    */

    take:
        slide
        cubeAlignmentTimestamps

    main:
        channel.fromList(slide.getRoundNames()).set { roundNamesCh }

        // MAKE ILLUMINATION PROFILES ==================================================================================
        GET_RANDOM_ILLUM_TILES_BY_ROUND(slide, roundNamesCh)
        MAKE_ILLUM_PROFILES_BY_ROUND(GET_RANDOM_ILLUM_TILES_BY_ROUND.out)
        MAKE_ILLUM_PROFILES_BY_ROUND.out.profiles
            .set { roundsIllumProfilesCh }

        // ASSESS ILLUMINATION =========================================================================================
        GET_ALL_ILLUM_TILES_BY_ROUND(slide, roundNamesCh)
        GET_ALL_ILLUM_TILES_BY_ROUND.out
            .join(roundsIllumProfilesCh, by: 0)
            .map {[
                round: it[0],
                tilePaths: it[1],
                flatfieldPath: it[2],
                darkfieldPath: it[3]
            ]}
            .set { roundsToAssessIllumCh }
        ASSESS_ILLUM_PROFILES_BY_ROUND(roundsToAssessIllumCh)

    emit:
        illumProfilesByRound = roundsIllumProfilesCh
}