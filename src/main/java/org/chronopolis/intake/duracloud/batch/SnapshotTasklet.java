package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.amqp.RoutingKey;
import org.chronopolis.common.digest.Digest;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.ingest.pkg.Unit;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.messaging.factory.MessageFactory;
import org.chronopolis.messaging.pkg.PackageReadyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by shake on 9/19/14.
 */
public class SnapshotTasklet implements Tasklet {
    private final Logger log = LoggerFactory.getLogger(SnapshotTasklet.class);

    private String snapshotID;
    private String collectionName;
    private String depositor;
    private IntakeSettings settings;
    private ChronProducer producer;
    private MessageFactory messageFactory;


    public SnapshotTasklet(String snapshotID,
                           String collectionName,
                           String depositor,
                           IntakeSettings settings,
                           ChronProducer producer,
                           MessageFactory messageFactory) {
        this.snapshotID = snapshotID;
        this.collectionName = collectionName;
        this.depositor = depositor;
        this.settings = settings;
        this.producer = producer;
        this.messageFactory = messageFactory;
    }


    @Override
    public RepeatStatus execute(final StepContribution stepContribution, final ChunkContext chunkContext) throws Exception {
        ManifestBuilder builder = new ManifestBuilder();

        Path duraBase = Paths.get(settings.getDuracloudSnapshotStage());
        Path snapshotBase = duraBase.resolve(snapshotID);

        builder.setRoot(snapshotBase);
        builder.setWriteBase(Paths.get(settings.getBagStage()));
        builder.setDepositor(depositor);
        builder.setCompressed(true);
        builder.setMaxSize(100, Unit.GIGABYTE);
        builder.setName(collectionName);
        builder.setIngestionType(IngestionType.DPN);

        builder.loadManifest(Files.newBufferedReader(
                snapshotBase.resolve("manifest-sha256.txt"),
                Charset.defaultCharset()));
        builder.newScanPackage();

        // Send a notification for each package
        for (ChronPackage chronPackage : builder.getPackages()) {
            Digest digest = Digest.fromString(chronPackage.getMessageDigest());
            long size = chronPackage.getTotalSize();

            // even though we set this above...
            // also we'll probably want the save name to have this instead
            String saveName = (builder.isCompressed())
                    ? chronPackage.getSaveName() + ".tar"
                    : chronPackage.getSaveName();

            Path saveFile = Paths.get(settings.getBagStage(),
                    depositor,
                    saveName);

            // And get the relative location
            Path location = Paths.get(settings.getBagStage()).relativize(saveFile);

            log.info("Save file {}; Save Name {}", saveFile, chronPackage.getSaveName());

            PackageReadyMessage packageReadyMessage = messageFactory.packageReadyMessage(
                    depositor,
                    digest,
                    location.toString(), // This is the relative path
                    collectionName,      // (ingest may have a different mount)
                    size
            );

            producer.send(packageReadyMessage, RoutingKey.INGEST_BROADCAST.asRoute());

            // TODO: Also register with dpn if we need to


        }

        return RepeatStatus.FINISHED;
    }
}
