package org.chronopolis.intake.duracloud.batch;

import com.google.common.hash.Hashing;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.ingest.pkg.Unit;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import retrofit2.Call;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tasklet to handle bagging and updating of history to duracloud
 * <p/>
 * Created by shake on 11/12/15.
 */
public class BaggingTasklet implements Tasklet {

    private final Logger log = LoggerFactory.getLogger(BaggingTasklet.class);

    private final char DATA_BAG = 'D';
    private final String PARAM_PAGE_SIZE = "page_size";
    private final String PROTOCOL = "rsync";
    private final String ALGORITHM = "sha256";

    private String snapshotId;
    private String collectionName;
    private String depositor;
    private IntakeSettings settings;

    private BridgeAPI bridge;

    public BaggingTasklet(String snapshotId,
                          String collectionName,
                          String depositor,
                          IntakeSettings settings,
                          BridgeAPI bridge) {
        this.snapshotId = snapshotId;
        this.collectionName = collectionName;
        this.depositor = depositor;
        this.settings = settings;
        this.bridge = bridge;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        ManifestBuilder builder = new ManifestBuilder();
        BaggingHistory history = new BaggingHistory(snapshotId, false);

        Path duraBase = Paths.get(settings.getDuracloudSnapshotStage());
        Path snapshotBase = duraBase.resolve(snapshotId);

        // Set up the builder
        builder.setRoot(snapshotBase);
        builder.setWriteBase(Paths.get(settings.getBagStage()));
        builder.setDepositor(depositor);
        builder.setCompressed(true);
        builder.setMaxSize(250, Unit.GIGABYTE); // Max size defined by DPN, todo: externalize
        builder.setName(collectionName);
        builder.setIngestionType(IngestionType.DPN);

        // And bag (with a sha256 manifest)
        builder.loadManifest(Files.newBufferedReader(
                snapshotBase.resolve(settings.getDuracloudManifest()),
                Charset.defaultCharset()));
        builder.newScanPackage();

        // Send a notification for each package
        for (ChronPackage chronPackage : builder.getPackages()) {
            // If we compress the bag, add .tar to the save name
            String saveName = (builder.isCompressed())
                    ? chronPackage.getSaveName() + ".tar"
                    : chronPackage.getSaveName();

            Path saveFile = Paths.get(settings.getBagStage(),
                    depositor,
                    saveName);

            // And get the relative location
            Path location = Paths.get(settings.getBagStage())
                    .relativize(saveFile);

            log.info("Save file {}; Save Name {}", saveFile, chronPackage.getSaveName());

            // String tagDigest = getTagDigest(chronPackage.getBuildListenerWriter());
            String receipt = com.google.common.io.Files.hash(saveFile.toFile(),
                    Hashing.sha256())
                    .toString();
            log.info("Digest is {}", receipt);

            history.addBaggingData(chronPackage.getSaveName(), receipt);
        }

        // Save the bag to duracloud
        Call<HistorySummary> historyCall = bridge.postHistory(snapshotId, history);
        historyCall.execute();

        return RepeatStatus.FINISHED;
    }

}
