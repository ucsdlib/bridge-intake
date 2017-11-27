package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.concurrent.TrackingThreadPoolExecutor;
import org.chronopolis.common.storage.BagStagingProperties;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class to submit Cleaning tasks to.
 *
 * @author shake
 */
public class Bicarbonate {

    public final TrackingThreadPoolExecutor<Path> executor;
    public final BagStagingProperties stagingProperties;

    public Bicarbonate(BagStagingProperties stagingProperties) {
        this.stagingProperties = stagingProperties;
        this.executor = new TrackingThreadPoolExecutor<>(1, 2, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    public Optional<FutureTask<Path>> submit(Path bag) {
        return executor.submitIfAvailable(new Cleaner(bag, stagingProperties), bag);
    }

    public void shutdown() {
        executor.shutdownNow();
    }
}
