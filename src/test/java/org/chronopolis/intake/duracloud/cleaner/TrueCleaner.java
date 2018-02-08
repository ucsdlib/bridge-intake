package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;

import java.nio.file.Paths;

/**
 * cleaner which always returns true for testing
 *
 * @author shake
 */
public class TrueCleaner extends Cleaner {
    public TrueCleaner() {
        super(Paths.get("/dev/null"), new BagStagingProperties());
    }

    @Override
    public Boolean call() {
        return true;
    }
}
