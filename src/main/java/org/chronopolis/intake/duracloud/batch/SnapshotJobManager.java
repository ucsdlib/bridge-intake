package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.intake.duracloud.DataCollector;
import org.chronopolis.intake.duracloud.batch.support.APIHolder;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.DuracloudRequest;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
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

import java.util.List;
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
    private BaggingTasklet baggingTasklet;

    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
    private JobLauncher jobLauncher;

    private DataCollector collector;
    private APIHolder holder;

    // Instantiated per manager
    private ExecutorService executor;

    @Autowired
    public SnapshotJobManager(JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              JobLauncher jobLauncher,
                              APIHolder holder,
                              SnapshotTasklet snapshotTasklet,
                              BaggingTasklet baggingTasklet,
                              DataCollector collector) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.snapshotTasklet = snapshotTasklet;
        this.baggingTasklet = baggingTasklet;
        this.jobLauncher = jobLauncher;
        this.collector = collector;
        this.holder = holder;

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
        BagData data = collector.collectBagData(details.getSnapshotId());

        startJob(data.snapshotId(),
                data.depositor(),
                data.name());
    }

    private void startJob(String snapshotId, String depositor, String collectionName) {
        log.trace("Starting tasklet for snapshot {}", snapshotId);
        log.info("Tasklet {}", baggingTasklet == null);
        DateTimeFormatter fmt = ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC();
        Job job = jobBuilderFactory.get("bagging-job")
                .start(stepBuilderFactory.get("bagging-step")
                    .tasklet(baggingTasklet)
                    .build()
                ).build();

        JobParameters parameters = new JobParametersBuilder()
                .addString("snapshotId", snapshotId)
                .addString("depositor", depositor)
                .addString("collectionName", collectionName)
                // .addString("date", fmt.print(new DateTime()))
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

    /**
     * Start a standalone ReplicationTasklet
     *
     * We do it here just for consistency, even though it's not
     * part of the batch stuff
     *
     * @param details
     * @param receipts
     * @param settings
     */
    public void startReplicationTasklet(SnapshotDetails details, List<BagReceipt> receipts, IntakeSettings settings) {
        BagData data = collector.collectBagData(details.getSnapshotId());
        ReplicationTasklet task = new ReplicationTasklet(data, receipts, holder.bridge, holder.ingest, holder.dpn, settings);
        task.run();
    }

}
