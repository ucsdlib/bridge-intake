package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.ingest.bagger.BagModel;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.DuracloudRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by shake on 7/29/14.
 */
public class SnapshotJobManager {
    private final Logger log = LoggerFactory.getLogger(org.chronopolis.intake.duracloud.batch.SnapshotJobManager.class);

    private ExecutorService executor;

    private SnapshotProcessor processor;
    private SnapshotWriter writer;
    private IntakeSettings intakeSettings;

    private HashMap<String, BagModel> models;

    @Autowired
    public SnapshotJobManager(SnapshotProcessor processor,
                              SnapshotWriter writer,
                              IntakeSettings intakeSettings) {
        this.processor = processor;
        this.writer = writer;
        this.intakeSettings = intakeSettings;
        this.executor = Executors.newSingleThreadExecutor();
        this.models = new HashMap<>();
    }

    public void addSnapshotJob(DuracloudRequest bag) {
        log.trace("Adding job for bag {}", bag.getSnapshotID());
        System.out.println("Adding bag job");
        BagModel model = models.get(bag.getSnapshotID());
        if (model == null) {
            SnapshotThread thread = new SnapshotThread(bag);
            executor.submit(thread);
        } else {
            log.error("Already started thread for snapshot " + bag.getSnapshotID());
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public void destroy() throws Exception {
        System.out.println("Shutting down thread pools");
        executor.shutdown();

        if (!executor.isTerminated()) {
            executor.shutdownNow();
        }

        executor = null;
    }

    public class SnapshotThread implements Runnable {
        private final DuracloudRequest bag;

        public SnapshotThread(final DuracloudRequest bag) {
            this.bag = bag;
        }

        @Override
        public void run() {
            SnapshotReader reader = new SnapshotReader(bag, intakeSettings);
            System.out.println("Reading request to get model");
            BagModel model = reader.read();
            models.put(bag.getSnapshotID(), model);
            System.out.println("Processing model");
            model = processor.process(model);
            System.out.println("Writing model to bag");
            writer.write(model);
        }
    }
}
