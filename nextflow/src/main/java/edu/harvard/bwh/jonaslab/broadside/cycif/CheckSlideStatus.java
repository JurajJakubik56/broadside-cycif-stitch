package edu.harvard.bwh.jonaslab.broadside.cycif;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class CheckSlideStatus {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("Need to provide a path to slide!");
        } else if (args.length > 1) {
            throw new RuntimeException("Can't interpret multiple arguments; expecting exactly one");
        }

        Path path = Paths.get(args[0]);
        Slide slide = new Slide(path);
        System.out.println(slide.nextflowSummary);
    }
}