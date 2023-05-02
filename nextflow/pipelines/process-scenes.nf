import java.nio.file.Path
import java.nio.file.Paths

params.refChannelIndex = 0

// registration/stitching parameters
params.ashlarMaxShiftUM = 15
params.ashlarFilterSigma = 0
params.zarrTileSize = 2**12
params.zarrPyramidMaxTopLevelSize = 512
params.zarrPyramidDownscale = 2
params.tiffTileSize = 512

String ashlarOutputFormat = "cycle_{cycle}_channel_{channel}.tiff"
String ashlarOutputPattern = "cycle_*_channel_*.tiff"
String ashlarFirstChannel = "cycle_0_channel_0.tiff"

String newline = "\n"
String colSep = "\t"

process COLLECT_TILES_BY_SCENE_AND_ROUND {
    tag "scene: ${scene}, round: ${round}"

    input:
        val slide
        tuple \
            val(scene), \
            val(round)

    output:
        tuple \
            val(scene), \
            val(round), \
            path("tiles-${scene}-${round}.txt")

    exec:
        List<Path> tilePaths = slide.getScene(scene).getTilePathsForRound(round)
        Path dst = task.workDir.resolve("tiles-${scene}-${round}.txt")
        file(dst).text = tilePaths.join(newline)
}

process STACK_TILES_BY_SCENE_AND_ROUND {
    if (!workflow.stubRun) {
        conda "${params.condaEnvironmentPath}"
    }
    tag "scene: ${scene}, round: ${round}"
    memory "16 GB"
    cpus 4

    publishDir(
        path: "${params.logDir}/stack-tiles/",
        enabled: !workflow.stubRun,
        mode: "copy",
        pattern: "stack-tiles-*.dask-performance.html",
    )

    input:
        tuple \
            val(scene), \
            val(round), \
            path(flatfieldPath), \
            path(darkfieldPath), \
            path(tilesPath)

    output:
        tuple \
            val(scene), \
            val(round), \
            path(tilesPath), \
            path("stack-${scene}-${round}.ome.tiff"), emit: stacks
        path("stack-tiles-${scene}-${round}.dask-performance.html")

    script:
        Path darkDir = Paths.get(params.calibrationDir).resolve("dark")
        Path scalesShiftsDir = Paths.get(params.calibrationDir).resolve("cube-alignment").resolve("scales-shifts")
        """
        stack-tiles \
            --tiles-path "${tilesPath}" \
            --flatfield-path "${flatfieldPath}" \
            --darkfield-path "${darkfieldPath}" \
            --dark-dir "${darkDir}" \
            --scales-shifts-dir "${scalesShiftsDir}" \
            --dst "stack-${scene}-${round}.ome.tiff" \
            --n-cpus "${task.cpus}" \
            --memory-limit "${task.memory}" \
            --dask-report-filename "stack-tiles-${scene}-${round}.dask-performance.html"
        """

    stub:
        """
        touch "stack-${scene}-${round}.ome.tiff"
        touch "stack-tiles-${scene}-${round}.dask-performance.html"
        """
}

process COLLECT_STACKS_BY_ROUND {
    tag "round: ${round}"
    input:
        tuple val(round), val(stackPaths)

    output:
        tuple val(round), path("stacks-${round}.txt")

    exec:
        Path dst = task.workDir.resolve("stacks-${round}.txt")
        file(dst).text = stackPaths.join(newline)
}

process SORT_STACKS {
    // needed so that the cycles for each scene are in order
    tag "scene: ${scene}"

    input:
        tuple \
            val(scene), \
            val(rounds), \
            val(tilesPaths), \
            val(stackPaths)

    output:
        tuple \
            val(scene), \
            val(roundsSorted), \
            val(tilesPathsSorted), \
            val(stackPathsSorted)

    exec:
        /*
        This is really ugly; preferably we would like to sort all three lists using the
        first list as a key, but I can't be bothered to make it more complicated than it
        really is
        */
        roundsSorted = rounds.sort({ a, b -> a <=> b})
        tilesPathsSorted = tilesPaths.sort({ a, b -> a.name <=> b.name})
        stackPathsSorted = stackPaths.sort({ a, b -> a.name <=> b.name})
}

process COLLECT_STACKS_BY_SCENE {
    tag "scene: ${scene}"
    input:
        tuple val(scene), val(stackPaths)

    output:
        tuple val(scene), path("stacks-${scene}.txt")

    exec:
        Path dst = task.workDir.resolve("stacks-${scene}.txt")
        file(dst).text = stackPaths.join(newline)
}

process COLLECT_TILES_BY_SCENE {
    tag "scene: ${scene}"

    input:
        tuple \
            val(scene), \
            val(rounds), \
            val(tilesPaths)

    output:
        tuple val(scene), path("tiles-by-round-${scene}.tsv")

    exec:
        List<String> lines = [["round", "tiles-path"].join(colSep)]
        List<List> roundsPaths = [rounds, tilesPaths].transpose()
        roundsPaths = roundsPaths.sort({a, b -> a[0] <=> b[0]})
        for (roundPath in roundsPaths) {
            lines.add(roundPath.join(colSep))
        }
        Path dst = task.workDir.resolve("tiles-by-round-${scene}.tsv")
        file(dst).text = lines.join(newline)
}

process REGISTER_STITCH_AND_DOWNSCALE_SCENE {
    if (!workflow.stubRun) {
        conda "${params.condaEnvironmentPath}"
    }
    tag "scene: ${sceneName}"
    maxForks 2

    publishDir(
        path: "${sceneDir}",
        enabled: !workflow.stubRun,
        mode: "copy",
    )

    input:
        val slideName
        tuple \
            val(sceneName), \
            val(sceneDir), \
            path(stacksPath), \
            path(tilesPath)

    output:
        tuple \
            val(sceneName), \
            path("${params.omeZarrDirname}"), \
            path("${params.omeXmlFilename}")

    script:
        /*
        `write-ome-metadata` is here with `register-and-stitch` because the publish operation must happen after both
        commands
        */
        String omeImageName = "{\\\"slide\\\": \\\"${slideName}\\\", \\\"scene\\\": \\\"${sceneName}\\\"}"
        """
        register-and-stitch \
            --output-format "${ashlarOutputFormat}" \
            --stacks-path "${stacksPath}" \
            --align-channel "${params.refChannelIndex}" \
            --filter-sigma "${params.ashlarFilterSigma}" \
            --maximum-shift "${params.ashlarMaxShiftUM}"

        make-zarr-pyramid \
            --src-pattern "${ashlarOutputPattern}" \
            --dst "${params.omeZarrDirname}" \
            --tile-size "${params.zarrTileSize}" \
            --max-top-level-size "${params.zarrPyramidMaxTopLevelSize}" \
            --downscale "${params.zarrPyramidDownscale}"

        write-ome-metadata \
            --ashlar-first-channel "${ashlarFirstChannel}" \
            --stacks-path "${stacksPath}" \
            --tiles-path "${tilesPath}" \
            --ome-image-name "${omeImageName}" \
            --ome-xml-path "${params.omeXmlFilename}"
        """

    stub:
        """
        mkdir "${params.omeZarrDirname}"
        touch "${params.omeXmlFilename}"
        """
}

workflow PROCESS_SCENES {
    take:
        slide
        illumProfilesByRound

    main:
        channel.fromList(slide.getSceneNames()).set { scenesCh }

        COLLECT_TILES_BY_SCENE_AND_ROUND(
            slide,
            scenesCh
                .map { [it, slide.getScene(it).getRoundNames()] }
                .transpose()
                .map { [scene: it[0], round: it[1]] }
                .map { [it.scene, it.round] }
        )
        STACK_TILES_BY_SCENE_AND_ROUND(
            scenesCh
               .combine(illumProfilesByRound)
               .map { [scene: it[0], round: it[1], flatfield: it[2], darkfield: it[3]] }
               .filter { slide.getScene(it.scene).getRoundNames().contains(it.round) }
               .map { [it.scene, it.round, it.flatfield, it.darkfield] }
               .join(COLLECT_TILES_BY_SCENE_AND_ROUND.out, by: [0, 1])
        )

        COLLECT_STACKS_BY_ROUND(
            STACK_TILES_BY_SCENE_AND_ROUND.out
                .stacks
                .groupTuple(by: 1)  // rounds
                .map { [round: it[1], stacksPath: it[3]] }
                .map { [it.round, it.stacksPath] }
        )
        SORT_STACKS(
            STACK_TILES_BY_SCENE_AND_ROUND.out
                .stacks
                .groupTuple(by: 0)
        )
        SORT_STACKS.out
            .map { [
                scene: it[0],
                rounds: it[1],
                tilesPaths: it[2],
                stackPaths: it[3],
            ] }
            .set { roundPropsBySceneCh }

        COLLECT_STACKS_BY_SCENE(
            roundPropsBySceneCh
                .map { [scene: it.scene, stackPaths: it.stackPaths] }
        )
        COLLECT_TILES_BY_SCENE(
            roundPropsBySceneCh
                .map { [scene: it.scene, rounds: it.rounds, tilesPaths: it.tilesPaths ]}
        )

        // prepare all scene metadata in list of tuples
        scenesCh
            .map { [it, slide.getScene(it).path] }
            .join(COLLECT_STACKS_BY_SCENE.out, by: 0)
            .join(COLLECT_TILES_BY_SCENE.out, by: 0)
            .map { [
                scene: it[0],
                sceneDir: it[1],
                stacksPath: it[2],
                tilesPath: it[3]
            ] }
            .set { sceneStacksPathTilesPathCh }
        REGISTER_STITCH_AND_DOWNSCALE_SCENE(
            slide.name,
            sceneStacksPathTilesPathCh
                .map {[
                    it.scene,
                    it.sceneDir,
                    it.stacksPath,
                    it.tilesPath,
                ]}
        )

}
