package org.chronopolis.intake.duracloud.batch;

import com.google.common.annotations.VisibleForTesting;
import org.chronopolis.intake.duracloud.batch.bagging.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.check.Checker;
import org.chronopolis.intake.duracloud.batch.check.DepositorCheck;
import org.chronopolis.intake.duracloud.batch.ingest.ChronopolisIngest;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>
 * Created by shake on 7/29/14.
 */
public class SnapshotJobManager {
    private final Logger log = LoggerFactory.getLogger(SnapshotJobManager.class);

    private final ChronFactory chronFactory;
    private final BaggingFactory baggingFactory;
    private final DepositorCheck depositorCheck;

    private final ThreadPoolExecutor longIo;
    private final ThreadPoolExecutor shortIo;
    private final ConcurrentSkipListSet<String> processing;

    /**
     * Create a SnapshotJobManager
     *
     * @param chronFactory   the {@link ChronFactory} for creating Chronopolis tasks
     * @param baggingFactory the {@link BaggingFactory} for creating Bagging tasks
     * @param depositorCheck the {@link DepositorCheck} for validating Depositors exist
     */
    public SnapshotJobManager(ChronFactory chronFactory,
                              BaggingFactory baggingFactory,
                              DepositorCheck depositorCheck) {
        this(chronFactory, baggingFactory, depositorCheck,
                new ConcurrentSkipListSet<>(),
                new ThreadPoolExecutor(4, 4, 0, MILLISECONDS, new LinkedBlockingQueue<>()),
                new ThreadPoolExecutor(4, 4, 0, MILLISECONDS, new LinkedBlockingQueue<>()));
    }

    @VisibleForTesting
    SnapshotJobManager(ChronFactory chronFactory,
                       BaggingFactory baggingFactory,
                       DepositorCheck depositorCheck,
                       ConcurrentSkipListSet<String> processing,
                       ThreadPoolExecutor longIo,
                       ThreadPoolExecutor shortIo) {
        this.chronFactory = chronFactory;
        this.baggingFactory = baggingFactory;
        this.depositorCheck = depositorCheck;

        this.longIo = longIo;
        this.shortIo = shortIo;
        this.processing = processing;
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
     * @param data    additional details about the snapshot
     * @param context the Bridge which is currently being operated on
     */
    public void bagSnapshot(BagData data, BridgeContext context) {
        final String snapshotId = data.snapshotId();

        // good enough for now to check that we aren't processing a snapshot multiple times
        if (processing.add(snapshotId)) {
            BaggingTasklet bagger = baggingFactory.baggingTasklet(data, context);

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
     * @param data          additional details about the snapshot
     * @param details       the {@link SnapshotDetails} containing snapshot information
     * @param receipts      the bag receipts for the snapshot
     * @param bridgeContext the Bridge which is currently being operated on
     */
    public void startReplicationTasklet(final BagData data,
                                        final SnapshotDetails details,
                                        final List<BagReceipt> receipts,
                                        final BridgeContext bridgeContext) {
        final String snapshotId = data.snapshotId();

        // If we're pushing to dpn, let's make the differences here
        // -> Always push to chronopolis so we have a separate tasklet (NotifyChron or something)
        // -> If we're pushing to dpn, do a DPNReplication Tasklet
        // -> Else have a Tasklet for checking status in chronopolis
        if (!depositorCheck.test(data, bridgeContext) || !processing.add(snapshotId)) {
            return;
        }

        Checker check;
        CompletableFuture<Void> ingestFuture;
        ChronopolisIngest chronIngest = chronFactory.ingest(data, receipts, bridgeContext);

        check = chronFactory.check(data, receipts, bridgeContext);
        ingestFuture = CompletableFuture.runAsync(chronIngest, longIo);

        ingestFuture.thenRunAsync(check, longIo)
                .whenComplete((v, t) -> processing.remove(snapshotId));
    }

}
