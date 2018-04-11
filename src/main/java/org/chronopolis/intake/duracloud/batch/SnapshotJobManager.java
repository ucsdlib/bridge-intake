package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.DataCollector;
import org.chronopolis.intake.duracloud.batch.bagging.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.check.Checker;
import org.chronopolis.intake.duracloud.batch.check.ChronopolisCheck;
import org.chronopolis.intake.duracloud.batch.check.DpnCheck;
import org.chronopolis.intake.duracloud.batch.ingest.ChronopolisIngest;
import org.chronopolis.intake.duracloud.batch.ingest.DpnDigest;
import org.chronopolis.intake.duracloud.batch.ingest.DpnIngest;
import org.chronopolis.intake.duracloud.batch.ingest.DpnReplicate;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.IngestAPIProperties;
import org.chronopolis.rest.api.StagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Start a Tasklet based on the type of request that comes in
 * <p>
 * If needed we could break this up into two classes as we start to decompose some of the objects
 * That way we aren't overrun with overly large constructors, and are able to keep functionality
 * more concise. i.e. BaggingBuilder, IngestBuilder, or something of the like.
 * <p>
 * <p>
 * Created by shake on 7/29/14.
 */
public class SnapshotJobManager {
    private final Logger log = LoggerFactory.getLogger(SnapshotJobManager.class);

    // Autowired from the configuration
    private final Notifier notifier;
    private final DataCollector collector;
    private final Bicarbonate cleaningManager;
    private final BagProperties bagProperties;
    private final IntakeSettings intakeSettings;
    private final BagStagingProperties bagStagingProperties;

    private final BridgeAPI bridge;
    private final LocalAPI dpnLocal;
    private final BagService chronBags;
    private final StagingService chronStaging;

    // do we want an overseer TP which we use to say: job(x, y) is already running, REJECTED!
    // for the most part this functionality could be served by this class, could work ok
    // need ThreadPools instead of executor
    // one for io, one for http?
    // Instantiated per manager
    private ExecutorService executor;
    private final ConcurrentSkipListSet<String> processing;

    public SnapshotJobManager(Notifier notifier,
                              Bicarbonate cleaningManager,
                              BagProperties bagProperties,
                              IntakeSettings intakeSettings,
                              BagStagingProperties bagStagingProperties,
                              BridgeAPI bridge,
                              LocalAPI dpnLocal,
                              BagService chronBags,
                              StagingService chronStaging,
                              DataCollector collector) {
        this.bridge = bridge;
        this.dpnLocal = dpnLocal;
        this.chronBags = chronBags;
        this.chronStaging = chronStaging;
        this.notifier = notifier;
        this.collector = collector;
        this.bagProperties = bagProperties;
        this.intakeSettings = intakeSettings;
        this.cleaningManager = cleaningManager;
        this.bagStagingProperties = bagStagingProperties;

        this.executor = new ThreadPoolExecutor(4, 4, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.processing = new ConcurrentSkipListSet<>();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void destroy() {
        log.debug("Shutting down thread pools");
        executor.shutdownNow();
        executor = null;
    }

    /**
     * Start (or queue) bagging for a snapshot
     *
     * @param details the details of the snapshot
     */
    public void bagSnapshot(SnapshotDetails details) {
        try {
            BagData data = collector.collectBagData(details.getSnapshotId());
            final String snapshotId = data.snapshotId();

            // good enough for now to check that we aren't processing a snapshot multiple times
            if (processing.add(snapshotId)) {
                BaggingTasklet bagger = new BaggingTasklet(snapshotId, data.depositor(),
                        intakeSettings, bagProperties, bagStagingProperties, bridge, notifier);

                CompletableFuture.runAsync(bagger, executor)
                        .whenComplete((v, t) -> processing.remove(snapshotId));
            }
        } catch (IOException e) {
            log.error("Error reading from properties file for snapshot {}", details.getSnapshotId());
        }
    }

    /**
     * Start a standalone ReplicationTasklet
     * <p>
     * We do it here just for consistency, even though it's not
     * part of the batch stuff
     *
     * @param details           the details of the snapshot
     * @param receipts          the bag receipts for the snapshot
     * @param settings          the settings for our intake service
     * @param ingestProperties  the properties for chronopolis Ingest API configuration
     * @param stagingProperties the properties defining the bag staging area
     */
    public void startReplicationTasklet(SnapshotDetails details,
                                        List<BagReceipt> receipts,
                                        IntakeSettings settings,
                                        IngestAPIProperties ingestProperties,
                                        BagStagingProperties stagingProperties) {
        // If we're pushing to dpn, let's make the differences here
        // -> Always push to chronopolis so have a separate tasklet for that (NotifyChron or something)
        // -> If we're pushing to dpn, do a DPNReplication Tasklet
        // -> Else have a Tasklet for checking status in chronopolis
        BagData data;
        try {
            data = collector.collectBagData(details.getSnapshotId());
        } catch (IOException e) {
            log.error("Error reading from properties file for snapshot {}", details.getSnapshotId());
            return;
        }

        Checker check;
        Events eventsAPI = dpnLocal.getEventsAPI();
        BalustradeBag bags = dpnLocal.getBagAPI();
        BalustradeNode nodes = dpnLocal.getNodeAPI();
        BalustradeTransfers transfers = dpnLocal.getTransfersAPI();

        ChronopolisIngest ingest = new ChronopolisIngest(data, receipts, chronBags,
                chronStaging, settings, stagingProperties, ingestProperties);

        // todo: if this all becomes async, we'll need to track that we're working on a snapshot
        // so... similar to the above
        if (settings.pushDPN()) {
            // only want to execute this once
            DpnNodeWeighter weighter = new DpnNodeWeighter(nodes, settings, details);

            // hmmm
            receipts.forEach(receipt -> {
                DpnDigest dpnDigest = new DpnDigest(receipt, bags, settings);
                DpnIngest dpnIngest = new DpnIngest(data, receipt, bags, settings,
                        stagingProperties);
                DpnReplicate dpnReplicate = new DpnReplicate(data.depositor(), settings,
                        stagingProperties, transfers);

                CompletableFuture<List<Weight>> weights = CompletableFuture.supplyAsync(weighter);
                CompletableFuture.supplyAsync(dpnIngest)
                        .thenApply(dpnDigest)
                        .thenAcceptBoth(weights, dpnReplicate);
            });

            check = new DpnCheck(data, receipts, bridge, bags, eventsAPI, cleaningManager);
        } else {
            check = new ChronopolisCheck(data, receipts, bridge, chronBags, cleaningManager);
        }

        // Might tie these to futures, not sure yet. That way we won't block here.
        // TODO: If ingest fails, we probably won't want to run the check
        ingest.run();
        check.run();
    }

}
