package org.chronopolis.intake.duracloud.cleaner;

import com.google.common.io.MoreFiles;
import org.chronopolis.common.storage.BagStagingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * Class to remove data under a directory
 *
 * @author shake
 */
public class Cleaner implements Callable<Boolean> {

    private final Logger log = LoggerFactory.getLogger(Cleaner.class);

    private final Path relative;
    private final BagStagingProperties stagingProperties;

    public Cleaner(Path relative, BagStagingProperties stagingProperties) {
        this.relative = relative;
        this.stagingProperties = stagingProperties;
    }

    @Override
    public Boolean call() {
        String root = stagingProperties.getPosix().getPath();
        Path bag = Paths.get(root).resolve(relative);

        boolean success = rm(bag);

        try {
            Path parent = bag.getParent();
            // Sanity check (parent != root) and check that the dir is empty
            if (success && !parent.toString().equals(root) &&
                    MoreFiles.listFiles(parent).isEmpty()) {
                Files.delete(parent);
            }
        } catch (IOException e) {
            log.error("[{}] Error removing parent directory", relative, e);
        }

        return success;
    }

    /**
     * Helper to remove a file or directory given by a full path.
     *
     * @param path the full path of the object to remove
     * @return whether that path was successfully removed
     */
    protected boolean rm(Path path) {
        boolean success = true;
        try {
            log.info("[Cleaner] Attempting to remove {}", path);
            if (Files.exists(path)) {
                MoreFiles.deleteRecursively(path);
            } else {
                log.warn("[Cleaner] {} no longer exists, unable to remove", path);
            }
        } catch (IOException e) {
            log.error("[Cleaner] Error removing {} from staging", path, e);
            success = false;
        }
        return success;
    }
}
