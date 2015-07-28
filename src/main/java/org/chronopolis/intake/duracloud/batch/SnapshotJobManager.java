package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.intake.duracloud.model.DuracloudRequest;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Start a {@link SnapshotTasklet} based on the type of request that comes in
 *
 * Created by shake on 7/29/14.
 */
public class SnapshotJobManager {
    private final Logger log = LoggerFactory.getLogger(SnapshotJobManager.class);

    // Autowired from the configuration
    private SnapshotTasklet snapshotTasklet;

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private JobLauncher jobLauncher;

    // Instantiated per manager
    private ExecutorService executor;

    @Autowired
    public SnapshotJobManager(JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              JobLauncher jobLauncher,
                              SnapshotTasklet snapshotTasklet) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.snapshotTasklet = snapshotTasklet;
        this.jobLauncher = jobLauncher;

        this.executor = new ThreadPoolExecutor(8, 8, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    }

    @Deprecated
    public void startSnapshotTasklet(DuracloudRequest request) {
        startJob(request.getSnapshotID(),
                request.getDepositor(),
                request.getCollectionName());
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

    public void startSnapshotTasklet(SnapshotDetails details) {
        // TODO: Need the depositor and a good collection name
        startJob(details.getSnapshotId(),
                details.getSourceSpaceId(),
                "USER_PLACEHOLDER");
    }

    private void startJob(String snapshotId, String depositor, String collectionName) {
        log.trace("Starting tasklet for snapshot {}", snapshotId);
        DateTimeFormatter fmt = ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC();
        Job job = jobBuilderFactory.get("snapshot-job")
                .start(stepBuilderFactory.get("snapshot-step")
                    .tasklet(snapshotTasklet)
                    .build()
                ).build();

        JobParameters parameters = new JobParametersBuilder()
                .addString("snapshotId", snapshotId)
                .addString("depositor", depositor)
                .addString("collectionName", collectionName)
                .addString("date", fmt.print(new DateTime()))
                .toJobParameters();

        try {
            jobLauncher.run(job, parameters);
        } catch (JobExecutionAlreadyRunningException
                | JobRestartException
                | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException e) {
            log.error("Error launching job\n", e);
        }

    }
}
