package org.chronopolis.intake.duracloud.model;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;

/**
 * Created by shake on 8/1/14.
 */
public class BagDirectoryFilter implements DirectoryStream.Filter<Path> {
    @Override
    public boolean accept(final Path path) throws IOException {
        BasicFileAttributeView view = Files.getFileAttributeView(path,
                BasicFileAttributeView.class);

        return !view.readAttributes().isDirectory();

    }
}
