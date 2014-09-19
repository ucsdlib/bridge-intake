package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.DuracloudRequest;
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

    DuracloudRequest request;
    IntakeSettings settings;

    public SnapshotTasklet(DuracloudRequest request, IntakeSettings settings) {
        this.request = request;
        this.settings = settings;
    }


    @Override
    public RepeatStatus execute(final StepContribution stepContribution, final ChunkContext chunkContext) throws Exception {
        ManifestBuilder builder = new ManifestBuilder();

        Path duraBase = Paths.get(settings.getDuracloudSnapshotStage());
        Path snapshotBase = duraBase.resolve(request.getSnapshotID());

        builder.setRoot(snapshotBase);
        builder.setWriteBase(Paths.get(settings.getBagStage()));
        builder.setDepositor(request.getDepositor());
        // builder.setCompressed(false);
        // builder.setMaxSize();
        builder.setName(request.getCollectionName());
        builder.setIngestionType(IngestionType.DPN);

        builder.loadManifest(Files.newBufferedReader(
                snapshotBase.resolve("manifest-sha256.txt"),
                Charset.defaultCharset()));
        builder.newScanPackage();


        return null;
    }
}
