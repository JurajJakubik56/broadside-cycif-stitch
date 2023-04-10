package edu.harvard.bwh.jonaslab.broadside.cycif;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

class NamedPolygon {
    String name;
    private String wkt;
}

class SlideSpecification {
    NamedPolygon[] polygons;
    private String[] focusPoints;
    private Path protocolPath;
    private String objName;
}

public class Slide {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());

    public final Path path;
    public final String name;
    public final List<Scene> scenes;
    public final Path jsonPath;
    public final String summary;
    public final String nextflowSummary;

    public Slide(Path path) {
        this(path, null, null);
    }

    public Slide(Path path, Set<String> selectedSceneNames, Set<String> selectedRoundNames) {
        // validate inputs
        Path jsonPath = path.resolve(".slide.json");
        if (!Files.exists(path) | !Files.exists(jsonPath)) {
            throw new InvalidPathException(path.toString(), "Path is not a valid slide");
        }
        String name = path.getFileName().toString();

        // get scene names
        Set<String> sceneNamesFromJson = getSceneNamesFromJson(jsonPath);
        Set<String> sceneNamesFromFileSystem = getSceneNamesFromFileSystem(path);
        if (!Objects.equals(sceneNamesFromJson, sceneNamesFromFileSystem)) {
            log.warn("Mismatch between scenes in slide.json and scenes on filesystem; using filesystem");
        }
        List<String> allSceneNames = sceneNamesFromFileSystem.stream().sorted().toList();

        // validate scene names
        Set<String> foundSceneNames = new HashSet<>(sceneNamesFromFileSystem);
        if (selectedSceneNames != null) {
            // make copy since set operations in java are in-place
            Set<String> extraNames = new HashSet<>(selectedSceneNames);
            extraNames.removeAll(sceneNamesFromFileSystem);
            if (extraNames.size() != 0) {
                log.warn(String.format("Unrecognized scene names: %s", extraNames));
            }
            foundSceneNames.retainAll(selectedSceneNames);
        }

        // create scene objects for selected scene names
        List<Scene> scenes = foundSceneNames
                .stream()
                .map(sceneName -> new Scene(path.resolve(sceneName), selectedRoundNames))
                .sorted(Comparator.comparing(scene -> scene.name))
                .toList();

        // compute scene and round names
        List<String> sceneNames = scenes
                .stream()
                .map(it -> it.name)
                .sorted()
                .toList();
        List<String> roundNames = scenes
                .stream()
                .flatMap(scene -> scene.getRoundNames().stream())
                .distinct()
                .sorted()
                .toList();

        // compute summaries
        String simpleSceneSummaries = scenes
                .stream()
                .map(scene -> scene.summary)
                .collect(Collectors.joining("\n"));
        String nextflowSummary = "" +
                "slide:             " + name + "\n" +
                "location:          " + path + "\n" +
                "scenes found:      " + allSceneNames + "\n" +
                "scenes to process: " + sceneNames + "\n" +
                "rounds to process: " + roundNames + "\n" +
                simpleSceneSummaries;

        String detailedSceneSummaries = scenes
                .stream()
                .map(scene -> scene.detailedSummary)
                .collect(Collectors.joining("\n"));
        String summary = "" +
                "slide:    " + name + "\n" +
                "location: " + path + "\n" +
                "scenes:   " + allSceneNames + "\n" +
                detailedSceneSummaries;

        // assign read-only properties; this class is a dataclass
        this.path = path;
        this.name = name;
        this.jsonPath = jsonPath;
        this.scenes = scenes;
//        this.allSceneNames = allSceneNames;
//        this.sceneNames = sceneNames;
//        this.roundNames = roundNames;
        this.summary = summary;
        this.nextflowSummary = nextflowSummary;
    }

    private static Set<String> getSceneNamesFromJson(Path jsonPath) {
        Gson gson = new Gson();
        try (FileReader reader = new FileReader(jsonPath.toFile())) {
            SlideSpecification slideSpec = gson.fromJson(reader, SlideSpecification.class);
            return Arrays.stream(slideSpec.polygons)
                    .map(it -> it.name)
                    .collect(Collectors.toUnmodifiableSet());

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static Set<String> getSceneNamesFromFileSystem(Path slidePath) {
        HashSet<String> sceneNames = new HashSet<>();
        try (DirectoryStream<Path> scenePaths = Files.newDirectoryStream(slidePath)) {
            for (Path scenePath : scenePaths) {
                Path tilesPath = scenePath.resolve("tiles");
                if (Files.exists(tilesPath)) {
                    sceneNames.add(scenePath.getFileName().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return sceneNames;
    }

    public Scene getScene(String name) {
        for (Scene scene : scenes) {
            if (Objects.equals(scene.name, name)) {
                return scene;
            }
        }
        throw new NoSuchElementException(name);
    }

    public List<String> getSceneNames() {
        return scenes
                .stream()
                .map(it -> it.name)
                .toList();
    }

    public List<String> getRoundNames() {
        // instead of creating a new set, we iterate over each scene's round names to keep the order
        List<String> roundNames = new ArrayList<>();
        for (Scene scene : scenes) {
            for (String roundName : scene.getRoundNames()) {
                if (!roundNames.contains(roundName)) {
                    roundNames.add(roundName);
                }
            }
        }
        return roundNames;
    }

    public List<Path> getTilePathsForRound(String roundName) {
        if (!getRoundNames().contains(roundName)) {
            log.warn(String.format("formatNo tile paths found for round %s", roundName));
            return Collections.emptyList();
        }

        List<Path> tilePaths = new ArrayList<>();
        for (Scene scene : scenes) {
            tilePaths.addAll(scene.getTilePathsForRound(roundName));
        }
        return tilePaths;
    }
}