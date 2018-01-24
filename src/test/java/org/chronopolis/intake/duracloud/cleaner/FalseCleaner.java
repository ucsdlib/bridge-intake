package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;

import java.nio.file.Paths;

/**
 * cleaner which always returns false for testing
 *
 * @author shake
 */
public class FalseCleaner extends Cleaner {
    public FalseCleaner() {
        super(Paths.get("/dev/null"), new BagStagingProperties());
    }

    @Override
    public Boolean call() {
        return false;
    }
}
