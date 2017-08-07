package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.bag.SimpleNamingSchema;
import org.chronopolis.bag.UUIDNamingSchema;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.OnDiskTagFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.packager.DirectoryPackager;
import org.chronopolis.bag.packager.TarPackager;
import org.chronopolis.bag.partitioner.Bagger;
import org.chronopolis.bag.partitioner.BaggingResult;
import org.chronopolis.bag.writer.BagWriter;
import org.chronopolis.bag.writer.SimpleBagWriter;
import org.chronopolis.bag.writer.WriteResult;
import org.chronopolis.intake.duracloud.batch.support.DpnWriter;
import org.chronopolis.intake.duracloud.batch.support.DuracloudMD5;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.config.props.Chron;
import org.chronopolis.intake.duracloud.config.props.Duracloud;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Tasklet to handle bagging and updating of history to duracloud
 * <p/>
 * Created by shake on 11/12/15.
 */
public class BaggingTasklet implements Tasklet {

    private final Logger log = LoggerFactory.getLogger(BaggingTasklet.class);

    public static final String SNAPSHOT_CONTENT_PROPERTIES = "content-properties.json";
    public static final String SNAPSHOT_COLLECTION_PROPERTIES = ".collection-snapshot.properties";
    public static final String SNAPSHOT_MD5 = "manifest-md5.txt";
    public static final String SNAPSHOT_SHA = "manifest-sha256.txt";

    private final String TITLE = "Unable to create bag for %s";

    private String snapshotId;
    private String depositor;
    private IntakeSettings settings;
    private BagProperties bagProperties;

    private BridgeAPI bridge;
    private Notifier notifier;

    public BaggingTasklet(String snapshotId,
                          String depositor,
                          IntakeSettings settings,
                          BagProperties bagProperties,
                          BridgeAPI bridge,
                          Notifier notifier) {
        this.snapshotId = snapshotId;
        this.depositor = depositor;
        this.settings = settings;
        this.bagProperties = bagProperties;
        this.bridge = bridge;
        this.notifier = notifier;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Chron chron = settings.getChron();
        Duracloud dc = settings.getDuracloud();

        Path duraBase = Paths.get(dc.getSnapshots());
        Path out = Paths.get(chron.getBags(), depositor);
        Path snapshotBase = duraBase.resolve(snapshotId);
        String manifestName = dc.getManifest();

        // Create the manifest to be used later on
        PayloadManifest manifest = PayloadManifest.loadFromStream(
                Files.newInputStream(snapshotBase.resolve(manifestName)),
                snapshotBase);

        if (manifest.getFiles().isEmpty()) {
            log.warn("{} - snapshot is empty!", snapshotId);
            notifier.notify(String.format(TITLE, snapshotId), "Snapshot contains no files and is unable to be bagged");
        } else {
            prepareBags(snapshotBase, out, manifest);
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * Prepare and write bags for a snapshot
     *
     * @param snapshotBase The base directory of the snapshot
     * @param out The output directory to write to
     * @param manifest The PayloadManifest of all files in the snapshot
     * @throws IOException If there's an error writing or communicating with the bridge
     */
    private void prepareBags(Path snapshotBase, Path out, PayloadManifest manifest) throws IOException {
        // TODO: fill out with what...?
        BagInfo info = new BagInfo()
                .includeMissingTags(true)
                .withInfo(BagInfo.Tag.INFO_SOURCE_ORGANIZATION, depositor);

        Bagger bagger = new Bagger()
                .withBagInfo(info)
                .withBagit(new BagIt())
                .withPayloadManifest(manifest)
                .withMaxSize(bagProperties.getMaxSize(), bagProperties.getUnit())
                .withTagFile(new DuracloudMD5(snapshotBase.resolve(SNAPSHOT_MD5)))
                .withTagFile(new OnDiskTagFile(snapshotBase.resolve(SNAPSHOT_CONTENT_PROPERTIES)))
                .withTagFile(new OnDiskTagFile(snapshotBase.resolve(SNAPSHOT_COLLECTION_PROPERTIES)));
        bagger = configurePartitioner(bagger, settings.pushDPN());

        BaggingResult partition = bagger.partition();
        if (partition.isSuccess()) {
            BagWriter writer = settings.pushDPN() ? buildDpnWriter(out) : buildWriter(out);
            List<WriteResult> results = writer.write(partition.getBags());
            updateBridge(results);
        } else {
            // do some logging of the failed bags
            log.error("{} - unable to partition bags! {} Invalid Files", snapshotId, partition.getRejected());
            String message = "Snapshot was not able to be partitioned." + partition.getRejected().size() + " Rejected Files";
            notifier.notify(String.format(TITLE, snapshotId), message);
        }
    }

    /**
     * Update the bridge with the results of our bagging if we succeeded
     *
     * @param results The results from writing the bags
     * @throws IOException If there's an exception communicating with the bridge
     */
    private void updateBridge(List<WriteResult> results) throws IOException {
        BaggingHistory history = new BaggingHistory(snapshotId, false);
        // we could filter -> map -> consume instead
        results.stream().filter(WriteResult::isSuccess)
                .map(w -> new BagReceipt()
                        .setName(w.getBag().getName())
                        .setReceipt(w.getReceipt()))
                .forEach(history::addBaggingData);

        if (results.size() == history.getHistory().size()) {
            Call<HistorySummary> hc = bridge.postHistory(snapshotId, history);
            hc.execute();
        } else {
            log.error("Error writing bags for {}", snapshotId);
            String message = "Unable to write bags for snapshot, "
                    + history.getHistory().size()
                    + " out of " + results.size()
                    + " succeeded";
            notifier.notify(String.format(TITLE, snapshotId), message);
        }
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
    private Bagger configurePartitioner(Bagger bagger, boolean dpn) {
        if (dpn) {
            bagger.withNamingSchema(new UUIDNamingSchema());
        } else {
            bagger.withNamingSchema(new SimpleNamingSchema(snapshotId));
        }
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

    /**
     * Build a bag writer which curates content for DPN
     * and writes a serialized bag
     *
     * @param out the location to write to
     * @return the DpnWriter
     */
    private BagWriter buildDpnWriter(Path out) {
        return new DpnWriter(depositor, snapshotId)
                .validate(true)
                .withPackager(new TarPackager(out));
    }

}
