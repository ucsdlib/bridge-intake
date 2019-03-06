package org.chronopolis.intake.duracloud.batch.bagging;

import org.chronopolis.bag.SimpleNamingSchema;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.OnDiskTagFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.metrics.Metric;
import org.chronopolis.bag.metrics.WriteMetrics;
import org.chronopolis.bag.packager.DirectoryPackager;
import org.chronopolis.bag.partitioner.Bagger;
import org.chronopolis.bag.partitioner.BaggingResult;
import org.chronopolis.bag.writer.BagWriter;
import org.chronopolis.bag.writer.SimpleBagWriter;
import org.chronopolis.bag.writer.WriteResult;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.intake.duracloud.batch.support.DuracloudMD5;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

/**
 * Tasklet to handle bagging and updating of history to duracloud
 * <p/>
 * Created by shake on 11/12/15.
 */
public class BaggingTasklet implements Runnable {

    private final Logger log;

    public static final String SNAPSHOT_CONTENT_PROPERTIES = "content-properties.json";
    public static final String SNAPSHOT_COLLECTION_PROPERTIES = ".collection-snapshot.properties";
    public static final String SNAPSHOT_MD5 = "manifest-md5.txt";

    private String snapshotId;
    private String depositor;
    private BagProperties bagProperties;
    private BagStagingProperties stagingProperties;

    private Notifier notifier;
    private BridgeContext bridgeContext;

    public BaggingTasklet(String snapshotId,
                          String depositor,
                          BridgeContext bridgeContext,
                          BagProperties bagProperties,
                          BagStagingProperties stagingProperties,
                          Notifier notifier) {
        this.notifier = notifier;
        this.depositor = depositor;
        this.snapshotId = snapshotId;
        this.bridgeContext = bridgeContext;
        this.bagProperties = bagProperties;
        this.stagingProperties = stagingProperties;
        this.log = bridgeContext.getLogger();
    }

    @Override
    public void run() {
        Posix posix = stagingProperties.getPosix();

        Path out = Paths.get(posix.getPath(), depositor);
        Path duraBase = Paths.get(bridgeContext.getSnapshots());
        Path snapshotBase = duraBase.resolve(snapshotId);
        String manifestName = bridgeContext.getManifest();

        // Create the manifest to be used later on
        try (InputStream input = Files.newInputStream(snapshotBase.resolve(manifestName))) {
            PayloadManifest manifest = PayloadManifest.loadFromStream(input, snapshotBase);
            if (manifest.getFiles().isEmpty()) {
                log.warn("{} - snapshot is empty!", snapshotId);

                String title = "Snapshot Error %s: No Files";
                String errorMsg = "Snapshot contains no files and is unable to be bagged";
                notifier.notify(String.format(title, snapshotId), errorMsg);
            } else {
                prepareBags(snapshotBase, out, manifest);
            }
        } catch (IOException e) {
            log.error("{} - unable to read manifest", snapshotId, e);
        }
    }

    /**
     * Prepare and write bags for a snapshot
     *
     * @param snapshotBase The base directory of the snapshot
     * @param out          The output directory to write to
     * @param manifest     The PayloadManifest of all files in the snapshot
     */
    private void prepareBags(Path snapshotBase, Path out, PayloadManifest manifest) {
        // TODO: fill out with what...?
        BagInfo info = new BagInfo()
                .includeMissingTags(true)
                .withInfo(BagInfo.Tag.INFO_SOURCE_ORGANIZATION, depositor);

        Path duracloudManifest = snapshotBase.resolve(SNAPSHOT_MD5);
        Path contentProperties = snapshotBase.resolve(SNAPSHOT_CONTENT_PROPERTIES);
        Path collectionProperties = snapshotBase.resolve(SNAPSHOT_COLLECTION_PROPERTIES);
        Bagger bagger = new Bagger()
                .withBagInfo(info)
                .withBagit(new BagIt())
                .withPayloadManifest(manifest)
                .withMaxSize(bagProperties.getMaxSize(), bagProperties.getUnit())
                .withTagFile(new DuracloudMD5(duracloudManifest, bridgeContext))
                .withTagFile(new OnDiskTagFile(contentProperties))
                .withTagFile(new OnDiskTagFile(collectionProperties));
        bagger = configurePartitioner(bagger);

        BaggingResult partition = bagger.partition();
        if (partition.isSuccess()) {
            BagWriter writer = buildWriter(out);
            List<WriteResult> results = writer.write(partition.getBags());
            updateBridge(results);
        } else {
            // do some logging of the failed bags
            log.error("{} - unable to partition bags! {} Invalid Files",
                    snapshotId, partition.getRejected());

            String title = "Snapshot Error %s: Unable to partition";
            String message = "Snapshot was not able to be partitioned."
                    + partition.getRejected().size() + " Rejected Files";
            notifier.notify(String.format(title, snapshotId), message);
        }
    }

    /**
     * Update the bridge with the results of our bagging if we succeeded
     * <p>
     * Not sure if we need to return the response, but we'll do it for now in case we end up
     * needing it.
     *
     * @param results The results from writing the bags
     * @return the response from communicating with the bridge
     */
    @SuppressWarnings("UnusedReturnValue")
    private Optional<HistorySummary> updateBridge(List<WriteResult> results) {
        BridgeAPI bridge = bridgeContext.getApi();
        Optional<HistorySummary> response = Optional.empty();
        SimpleCallback<HistorySummary> summaryCB = new SimpleCallback<>();
        BaggingHistory history = new BaggingHistory(snapshotId, false);

        results.stream()
                .filter(WriteResult::isSuccess)
                .peek(this::captureMetrics)
                .map(w -> new BagReceipt()
                        .setName(w.getBag().getName())
                        .setReceipt(w.getReceipt()))
                .forEach(history::addBaggingData);

        if (results.size() == history.getHistory().size()) {
            Call<HistorySummary> hc = bridge.postHistory(snapshotId, history);
            hc.enqueue(summaryCB);
            response = summaryCB.getResponse();
        } else {
            log.error("Error writing bags for {}", snapshotId);
            String title = "Unable to create bag for %s";
            String message = "Unable to write bags for snapshot, "
                    + history.getHistory().size()
                    + " out of " + results.size()
                    + " succeeded";
            notifier.notify(String.format(title, snapshotId), message);
        }

        return response;
    }

    private void captureMetrics(WriteResult result) {
        Logger logger = LoggerFactory.getLogger("metrics");
        WriteMetrics metrics = result.getMetrics();
        if (metrics != null) {
            String bag = result.getBag().getName();
            logMetric(logger, bag, "bag", metrics.getBag());
            logMetric(logger, bag, "manifest", metrics.getManifest());
            logMetric(logger, bag, "tagmanifest", metrics.getTagmanifest());
            logMetric(logger, bag, "payload", metrics.getPayload());
            metrics.getPayloadFiles()
                    .forEach(metric -> logMetric(logger, bag, "payload-file", metric));
            metrics.getExtraTags()
                    .forEach(metric -> logMetric(logger, bag, "tag-file", metric));
        }
    }

    private void logMetric(Logger log, String bag, String type, Metric metric) {
        log.info("{},{},{},{},{}", bag, type,
                metric.getElapsed(),
                metric.getFilesWritten(),
                metric.getBytesWritten());
    }

    /**
     * Update the Bagger partitioner based on if we are pushing to dpn or not
     * <p>
     * If we are going to dpn, abide by the limitations they have set
     *
     * @param bagger the Bagger to update
     * @param dpn    boolean flag indicating if we're dpn bound
     * @return The updated Bagger
     */
    private Bagger configurePartitioner(Bagger bagger) {
        bagger.withNamingSchema(new SimpleNamingSchema(snapshotId));
        return bagger;
    }

    /**
     * Build a writer which only uses a directory packager
     *
     * @param out the location to write to
     * @return the BagWriter
     */
    private BagWriter buildWriter(Path out) {
        return new SimpleBagWriter()
                .validate(true)
                .withPackager(new DirectoryPackager(out));
    }

}
