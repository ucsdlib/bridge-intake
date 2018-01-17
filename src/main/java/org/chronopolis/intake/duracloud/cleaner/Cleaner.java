package org.chronopolis.intake.duracloud.cleaner;

import com.google.common.io.MoreFiles;
import org.chronopolis.common.storage.BagStagingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Class to remove data under a directory
 *
 * todo: return the status of the deletion? anyway to tell progress? log it?
 *
 * @author shake
 */
public class Cleaner implements Runnable {

    private final Logger log = LoggerFactory.getLogger(Cleaner.class);

    private final Path relative;
    private final BagStagingProperties stagingProperties;

    public Cleaner(Path relative, BagStagingProperties stagingProperties) {
        this.relative = relative;
        this.stagingProperties = stagingProperties;
    }

    @Override
    public void run() {
        boolean success = false;
        String root = stagingProperties.getPosix().getPath();
        Path bag = Paths.get(root).resolve(relative);

        log.info("[{}] Attempting to remove staged content from {}", relative, root);
        try {
            if (Files.exists(bag)) {
                MoreFiles.deleteRecursively(bag);
                success = true;
            } else {
                log.warn("[{}] Bag no longer exists, unable to remove", relative);
            }
        } catch (IOException e) {
            log.error("[{}] Error removing bag from staging", relative, e);
        }

        Path parent = bag.getParent();
        try {
            // Sanity check (parent != root) and check that the dir is empty
            if (success && !parent.equals(root) && MoreFiles.listFiles(parent).isEmpty()) {
                Files.delete(parent);
            }
        } catch (IOException e) {
            log.error("[{}] Error removing parent directory", relative, e);
        }
    }
}
