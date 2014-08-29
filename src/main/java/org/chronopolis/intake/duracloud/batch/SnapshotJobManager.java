package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.db.intake.StatusRepository;
import org.chronopolis.ingest.bagger.BagModel;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.DuracloudRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;

/**
 * Created by shake on 7/29/14.
 */
public class SnapshotJobManager {
    private final Logger log = LoggerFactory.getLogger(SnapshotJobManager.class);

    // From beans
    private StatusRepository statusRepository;
    private SnapshotProcessor processor;
    private SnapshotWriter writer;
    private IntakeSettings intakeSettings;

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private JobLauncher jobLauncher;

    // Instantiated per manager
    private ExecutorService executor;
    private HashMap<String, BagModel> models;

    @Autowired
    public SnapshotJobManager(SnapshotProcessor processor,
                              SnapshotWriter writer,
                              IntakeSettings intakeSettings,
                              StatusRepository statusRepository) {
        this.processor = processor;
        this.writer = writer;
        this.intakeSettings = intakeSettings;
        this.statusRepository = statusRepository;

        this.models = new HashMap<>();
    }

    public void addSnapshotJob(DuracloudRequest bag) {
        log.trace("Adding job for bag {}", bag.getSnapshotID());
        Job job = jobBuilderFactory.get("snapshot-job")
                .start(stepBuilderFactory.get("step1")
                    .<BagModel, BagModel> chunk(1)
                    .reader(new SnapshotReader(bag, intakeSettings, statusRepository))
                    .processor(processor)
                    .writer(writer)
                    .build())
                .build();

        JobParameters parameters = new JobParametersBuilder()
                .addString("snapshot-id", bag.getSnapshotID())
                .toJobParameters();

        try {
            jobLauncher.run(job, parameters);
        } catch (JobExecutionAlreadyRunningException
                | JobRestartException
                | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException e) {
            log.error("Error launching job\n", e);
        }

        /*
        BagModel model = models.get(bag.getSnapshotID());
        if (model == null) {
            SnapshotThread thread = new SnapshotThread(bag);
            executor.submit(thread);
        } else {
            log.error("Already started thread for snapshot " + bag.getSnapshotID());
        }
        */
    }

    @SuppressWarnings("UnusedDeclaration")
    public void destroy() throws Exception {
        log.debug("Shutting down thread pools");
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
            SnapshotReader reader = new SnapshotReader(bag, intakeSettings, statusRepository);
            BagModel model = reader.read();
            models.put(bag.getSnapshotID(), model);
            // model = processor.process(model);
            // writer.write(model);
        }
    }
}
