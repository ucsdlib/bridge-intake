package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Member;
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
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.api.FileService;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Depositor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Start a Tasklet based on the type of request that comes in
 * <p>
 * If needed we could break this up into two classes as we start to decompose some of the objects
 * That way we aren't overrun with overly large constructors, and are able to keep functionality
 * more concise. i.e. BaggingBuilder, IngestBuilder, or something of the like.
 * <p>
 * todo: we need some tests which interrupt tasks while they are running (i.e. a shutdown)
 * todo: keep track of failed depositors so we don't notify more than once. when a depositor check
 * succeeds, see if we should remove them from the failed set. maybe something the notifier should
 * do.
 * <p>
 * Created by shake on 7/29/14.
 */
public class SnapshotJobManager {
    private final Logger log = LoggerFactory.getLogger(SnapshotJobManager.class);

    // Autowired from the configuration
    private final Notifier notifier;
    private final Bicarbonate cleaningManager;
    private final BagProperties bagProperties;
    private final BagStagingProperties bagStagingProperties;

    private final LocalAPI dpnLocal;
    private final BagService chronBags;
    private final FileService chronFiles;
    private final DepositorService depositors;
    private final StagingService chronStaging;

    // do we want an overseer TP which we use to say: job(x, y) is already running, REJECTED!
    // for the most part this functionality could be served by this class, could work ok
    // need ThreadPools instead of executor
    // one for io, one for http?
    // Instantiated per manager
    private final ThreadPoolExecutor longIo;
    private final ThreadPoolExecutor shortIo;
    private final ConcurrentSkipListSet<String> processing;

    /**
     * Create a SnapshotJobManager
     *
     * @param notifier             the notification services on the event of failure
     * @param cleaningManager      the class to clean staging areas
     * @param bagProperties        configuration properties for BagIt
     * @param bagStagingProperties configuration properties for local staging areas
     * @param dpnLocal             the local DPN APIs
     * @param chronBags            the API to access Bags in Chronopolis
     * @param chronStaging         the API to access registered staging areas in Chronopolis
     * @param depositors           the API to access Depositors in Chronopolis
     */
    public SnapshotJobManager(Notifier notifier,
                              Bicarbonate cleaningManager,
                              BagProperties bagProperties,
                              BagStagingProperties bagStagingProperties,
                              LocalAPI dpnLocal,
                              BagService chronBags,
                              FileService chronFiles,
                              StagingService chronStaging,
                              DepositorService depositors) {
        this.dpnLocal = dpnLocal;
        this.chronBags = chronBags;
        this.chronStaging = chronStaging;
        this.notifier = notifier;
        this.bagProperties = bagProperties;
        this.cleaningManager = cleaningManager;
        this.bagStagingProperties = bagStagingProperties;
        this.depositors = depositors;
        this.chronFiles = chronFiles;

        this.processing = new ConcurrentSkipListSet<>();
        this.longIo = new ThreadPoolExecutor(4, 4, 0, MILLISECONDS, new LinkedBlockingQueue<>());
        this.shortIo = new ThreadPoolExecutor(4, 4, 0, MILLISECONDS, new LinkedBlockingQueue<>());
    }

    /**
     * Shutdown all resources associated with this ${@link SnapshotJobManager}
     */
    @SuppressWarnings("UnusedDeclaration")
    public void destroy() {
        log.debug("Shutting down thread pools");
        longIo.shutdownNow();
        shortIo.shutdownNow();
    }

    /**
     * Start (or queue) bagging for a snapshot
     *
     * @param data          additional details about the snapshot
     * @param bridgeContext the Bridge which is currently being operated on
     */
    public void bagSnapshot(BagData data, BridgeContext bridgeContext) {
        final String snapshotId = data.snapshotId();

        // good enough for now to check that we aren't processing a snapshot multiple times
        if (processing.add(snapshotId)) {
            BaggingTasklet bagger = new BaggingTasklet(snapshotId,
                    data.depositor(),
                    bridgeContext,
                    bagProperties,
                    bagStagingProperties,
                    notifier);

            CompletableFuture.runAsync(bagger, longIo)
                    .whenComplete((v, t) -> processing.remove(snapshotId));
        }
    }

    /**
     * Start a standalone ReplicationTasklet
     * <p>
     * We do it here just for consistency, even though it's not
     * part of the batch stuff
     *
     * @param data              additional details about the snapshot
     * @param receipts          the bag receipts for the snapshot
     * @param settings          the settings for our intake service
     * @param bridgeContext     the Bridge which is currently being operated on
     * @param stagingProperties the properties defining the bag staging area
     */
    public void startReplicationTasklet(final BagData data,
                                        final List<BagReceipt> receipts,
                                        final IntakeSettings settings,
                                        final BridgeContext bridgeContext,
                                        final BagStagingProperties stagingProperties) {
        // If we're pushing to dpn, let's make the differences here
        // -> Always push to chronopolis so we have a separate tasklet (NotifyChron or something)
        // -> If we're pushing to dpn, do a DPNReplication Tasklet
        // -> Else have a Tasklet for checking status in chronopolis
        if (!processing.add(data.snapshotId())) {
            return;
        }

        if (!checkDepositor(data, bridgeContext)) {
            return;
        }

        Checker check;
        Events events = dpnLocal.getEventsAPI();
        BridgeAPI bridge = bridgeContext.getApi();
        BalustradeBag bags = dpnLocal.getBagAPI();
        final String snapshotId = data.snapshotId();

        CompletableFuture<Void> ingestFuture;
        ChronopolisIngest chronIngest = new ChronopolisIngest(data, receipts, chronBags,
                chronStaging, settings, stagingProperties, chronFiles, depositors, bridgeContext);

        if (bridgeContext.getPush() == Push.DPN) {
            check = new DpnCheck(data, receipts, bridgeContext, bridge,
                    bags, events, depositors, cleaningManager, settings);

            // Also need to do DPN Ingest steps
            CompletableFuture<Void> dpnIngest = dpnIngest(data, receipts, dpnLocal, bridgeContext,
                    settings, stagingProperties);
            ingestFuture = dpnIngest.thenRunAsync(chronIngest, longIo);
        } else {
            check = new ChronopolisCheck(data, receipts, bridgeContext,
                    bridge, depositors, cleaningManager);
            ingestFuture = CompletableFuture.runAsync(chronIngest, longIo);
        }

        ingestFuture.thenRunAsync(check, longIo)
                .whenComplete((v, t) -> processing.remove(snapshotId));
    }

    /**
     * Create a CompletableFuture for the steps required to ingest content into DPN
     *
     * @param data              the bag data
     * @param receipts          the bag receipts
     * @param localAPI          the DPN APIs
     * @param settings          the intake settings
     * @param properties the staging properties
     * @return the CompletableFuture
     */
    private CompletableFuture<Void> dpnIngest(BagData data,
                                              List<BagReceipt> receipts,
                                              LocalAPI localAPI,
                                              BridgeContext context,
                                              IntakeSettings settings,
                                              BagStagingProperties properties) {
        String dep = data.depositor();
        BalustradeBag bags = localAPI.getBagAPI();
        BalustradeNode nodes = localAPI.getNodeAPI();
        BalustradeTransfers transfers = localAPI.getTransfersAPI();

        int i = 0;
        CompletableFuture[] futures = new CompletableFuture[receipts.size()];
        DpnNodeWeighter weighter = new DpnNodeWeighter(data, nodes, settings);

        for (BagReceipt receipt : receipts) {
            DpnDigest dpnDigest = new DpnDigest(receipt, context, bags, settings);
            DpnIngest dpnIngest = new DpnIngest(data, receipt, context, bags, settings, properties);
            DpnReplicate dpnReplicate = new DpnReplicate(dep, context,
                    settings, properties, transfers);

            CompletableFuture<List<Weight>> weights =
                    CompletableFuture.supplyAsync(() -> weighter.get(context.getLogger()));
            futures[i++] = CompletableFuture.supplyAsync(dpnIngest, shortIo)
                    .thenApplyAsync(dpnDigest, shortIo)
                    .thenAcceptBothAsync(weights, dpnReplicate, shortIo);
        }

        return CompletableFuture.allOf(futures);
    }

    /**
     * Simple validation to ensure that a given depositor exists in Chronopolis and DPN if required
     *
     * @param bagData       the BagData containing the name and uuid of the depositor
     * @param bridgeContext the Bridge which is currently being operated on
     * @return true if the member if found in Chronopolis (and DPN if required)
     */
    private boolean checkDepositor(BagData bagData, BridgeContext bridgeContext) {
        // For verifying that a member exists in chronopolis and/or dpn
        boolean exists = true;
        StringBuilder message = new StringBuilder();
        SimpleCallback<Depositor> chronCallback = new SimpleCallback<>();

        String depositor = bagData.depositor();
        Call<Depositor> chronDepositor = depositors.getDepositor(depositor);
        chronDepositor.enqueue(chronCallback);

        message.append("Snapshot Id: ").append(bagData.snapshotId()).append("\n");
        if (!chronCallback.getResponse().isPresent()) {
            exists = false;
            message.append("Chronopolis Depositor ")
                    .append(depositor)
                    .append(" is missing\n");
        }

        if (bridgeContext.getPush() == Push.DPN) {
            String member = bagData.member();
            SimpleCallback<Member> dpnCallback = new SimpleCallback<>();
            Call<Member> memberCall = dpnLocal.getMemberAPI().getMember(member);
            memberCall.enqueue(dpnCallback);
            if (!dpnCallback.getResponse().isPresent()) {
                exists = false;
                message.append("DPN Member ").append(member).append(" is missing\n");
            }
        }

        if (!exists) {
            log.warn(message.toString());
            notifier.notify("Missing depositor " + depositor, message.toString());
        }

        return exists;
    }

}
