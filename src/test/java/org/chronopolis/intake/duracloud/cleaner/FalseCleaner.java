package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;
import org.slf4j.Logger;

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
    public Boolean apply(Logger log) {
        return false;
    }
}
