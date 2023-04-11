package edu.harvard.bwh.jonaslab.broadside.cycif;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Scene {
    private final Logger log = LoggerFactory.getLogger(getClass().getName());
    public final Path path;
    private final Path tilesPath;
    public final String name;
    //    private final List<String> allRoundNames;
    private final List<String> roundNames;
    public final String summary;
    public final String detailedSummary;

    private final static Pattern roundPattern = Pattern.compile("R.*");
    private final static Pattern tileFilenamePattern = Pattern.compile(".*\\.ome\\.tiff");

    public Scene(Path path) {
        this(path, null);
    }

    public Scene(Path path, Set<String> selectedRoundNames) {
        Path tilesPath = path.resolve("tiles");

        // validate inputs
        if (!Files.exists(path) | !Files.exists(tilesPath)) {
            throw new InvalidPathException(String.valueOf(path), "Path is not a valid scene");
        }
        String name = path.getFileName().toString();

        // get round names
        Set<String> fsRoundNames = getRoundNamesFromFileSystem(tilesPath);
        Set<String> roundNames = new HashSet<>(fsRoundNames);
        if (selectedRoundNames != null) {
            Set<String> extraRoundNames = new HashSet<>(selectedRoundNames);
            extraRoundNames.removeAll(fsRoundNames);
            if (extraRoundNames.size() != 0) {
                log.warn(String.format("Unrecognized round names found: %s", extraRoundNames));
            }
            roundNames.retainAll(selectedRoundNames);
        }
        // assign initial set of read-only properties
        this.path = path;
        this.name = name;
//        this.fsRoundNames = fsRoundNames.stream().sorted().collect(Collectors.toUnmodifiableList());
        this.tilesPath = tilesPath;
        this.roundNames = roundNames.stream().sorted().collect(Collectors.toUnmodifiableList());

        // compute summaries based on properties
        int maxRoundLen = 0;
        for (String roundName : this.roundNames) {
            maxRoundLen = Math.max(maxRoundLen, roundName.length());
        }

        String summary = String.format("Scene: %s (rounds found: %s)", name, this.roundNames);
        List<String> roundSummaries = new ArrayList<>();
        roundSummaries.add(String.format("Scene: %s", this.name));
        for (String roundName : this.roundNames) {
            String spacing = " " + " ".repeat(Math.max(0, maxRoundLen - roundName.length()));
            int nImages = getTilePathsForRound(roundName).size();
            roundSummaries.add(String.format(
                    "\n\t%s:%s%d tiles",
                    roundName,
                    spacing,
                    nImages
            ));
        }
        String detailedSummary = String.join("", roundSummaries);

        // assign other properties
        this.summary = summary;
        this.detailedSummary = detailedSummary;
    }

    private static Set<String> getRoundNamesFromFileSystem(Path tilesPath) {
        Set<String> roundNames = new HashSet<>();
        try (DirectoryStream<Path> roundPaths = Files.newDirectoryStream(tilesPath)) {
            for (Path roundPath : roundPaths) {
                if (!roundPath.toFile().isDirectory()) {
                    continue;
                }

                String dirName = roundPath.getFileName().toString();
                Matcher matcher = roundPattern.matcher(dirName);
                if (matcher.matches()) {
                    roundNames.add(dirName);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return roundNames;
    }

    public List<Path> getTilePathsForRound(String roundName) {
        if (!roundNames.contains(roundName)) {
            log.warn(String.format("No tile paths found for round %s", roundName));
            return Collections.emptyList();
        }

        File[] tilesAsFiles = tilesPath.resolve(roundName).toFile().listFiles();
        Stream<File> tilesAsStream = Arrays.stream(Objects.requireNonNull(tilesAsFiles));
        return tilesAsStream
                .map(File::toPath)
                .filter(it -> tileFilenamePattern.matcher(it.getFileName().toString()).matches())
                .collect(Collectors.toUnmodifiableList());
    }

    public List<String> getRoundNames() {
        return roundNames;
    }
}