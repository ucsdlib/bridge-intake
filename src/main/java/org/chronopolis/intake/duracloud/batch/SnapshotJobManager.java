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
    private ThreadPoolExecutor longIO;
    private ThreadPoolExecutor shortIO;
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

        this.processing = new ConcurrentSkipListSet<>();
        this.longIO = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        this.shortIO = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    @SuppressWarnings("UnusedDeclaration")
    public void destroy() {
        log.debug("Shutting down thread pools");
        longIO.shutdownNow();
        shortIO.shutdownNow();
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

                CompletableFuture.runAsync(bagger, longIO)
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

        if (!processing.add(data.snapshotId())) {
            return;
        }

        Checker check;
        BalustradeBag bags = dpnLocal.getBagAPI();
        Events eventsAPI = dpnLocal.getEventsAPI();
        final String snapshotId = data.snapshotId();

        CompletableFuture<Void> ingestFuture;
        ChronopolisIngest chronIngest = new ChronopolisIngest(data, receipts, chronBags,
                chronStaging, settings, stagingProperties, ingestProperties);

        if (settings.pushDPN()) {
            check = new DpnCheck(data, receipts, bridge, bags, eventsAPI, cleaningManager);

            // Also need to do DPN Ingest steps
            CompletableFuture<Void> dpnIngest = dpnIngest(data, details, receipts,
                    dpnLocal, settings, stagingProperties);
            ingestFuture = dpnIngest.thenRunAsync(chronIngest, longIO);
        } else {
            check = new ChronopolisCheck(data, receipts, bridge, chronBags, cleaningManager);
            ingestFuture = CompletableFuture.runAsync(chronIngest, longIO);
        }

        ingestFuture.thenRunAsync(check, longIO)
                .whenComplete((v, t) -> processing.remove(snapshotId));
    }

    /**
     * Create a CompletableFuture for the steps required to ingest content into DPN
     *
     * @param data              the bag data
     * @param details           the snapshot details
     * @param receipts          the bag receipts
     * @param localAPI          the DPN APIs
     * @param settings          the intake settings
     * @param stagingProperties the staging properties
     * @return the CompletableFuture
     */
    private CompletableFuture<Void> dpnIngest(BagData data,
                                              SnapshotDetails details,
                                              List<BagReceipt> receipts,
                                              LocalAPI localAPI,
                                              IntakeSettings settings,
                                              BagStagingProperties stagingProperties) {
        String dep = data.depositor();
        BalustradeBag bags = localAPI.getBagAPI();
        BalustradeNode nodes = localAPI.getNodeAPI();
        BalustradeTransfers transfers = localAPI.getTransfersAPI();

        int i = 0;
        CompletableFuture[] futures = new CompletableFuture[receipts.size()];
        DpnNodeWeighter weighter = new DpnNodeWeighter(nodes, settings, details);

        for (BagReceipt receipt : receipts) {
            DpnDigest dpnDigest = new DpnDigest(receipt, bags, settings);
            DpnIngest dpnIngest = new DpnIngest(data, receipt,
                    bags, settings, stagingProperties);
            DpnReplicate dpnReplicate = new DpnReplicate(dep, settings,
                    stagingProperties, transfers);

            CompletableFuture<List<Weight>> weights = CompletableFuture.supplyAsync(weighter);
            futures[i++] = CompletableFuture.supplyAsync(dpnIngest, shortIO)
                    .thenApplyAsync(dpnDigest, shortIO)
                    .thenAcceptBothAsync(weights, dpnReplicate, shortIO);
        }
        return CompletableFuture.allOf(futures);
    }

}
