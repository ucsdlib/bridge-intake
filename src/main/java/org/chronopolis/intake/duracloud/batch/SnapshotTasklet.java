package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.Multimap;
import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.common.dpn.DPNBag;
import org.chronopolis.common.dpn.DPNService;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.ingest.pkg.DpnBagWriter;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.ingest.pkg.Unit;
import org.chronopolis.ingest.pkg.Writer;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.messaging.factory.MessageFactory;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.IngestRequest;
import org.joda.time.DateTime;
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
import java.util.Map;

/**
 * Created by shake on 9/19/14.
 */
public class SnapshotTasklet implements Tasklet {
    private final Logger log = LoggerFactory.getLogger(SnapshotTasklet.class);

    private String snapshotID;
    private String collectionName;
    private String depositor;
    private IntakeSettings settings;


    // Services to talk with both Chronopolis and DPN
    private IngestAPI chronAPI;
    private DPNService dpnService;


    @Deprecated
    public SnapshotTasklet(String snapshotID,
                           String collectionName,
                           String depositor,
                           IntakeSettings settings,
                           ChronProducer producer,
                           MessageFactory messageFactory,
                           DPNService dpnService) {
        this.snapshotID = snapshotID;
        this.collectionName = collectionName;
        this.depositor = depositor;
        this.settings = settings;
        // this.producer = producer;
        // this.messageFactory = messageFactory;
        this.dpnService = dpnService;
    }

    public SnapshotTasklet(String snapshotID,
                           String collectionName,
                           String depositor,
                           IntakeSettings settings,
                           IngestAPI chronAPI,
                           DPNService dpnService) {
        this.snapshotID = snapshotID;
        this.collectionName = collectionName;
        this.depositor = depositor;
        this.settings = settings;

        this.chronAPI = chronAPI;
        this.dpnService = dpnService;
    }

    @Override
    public RepeatStatus execute(final StepContribution stepContribution, final ChunkContext chunkContext) throws Exception {
        ManifestBuilder builder = new ManifestBuilder();

        Path duraBase = Paths.get(settings.getDuracloudSnapshotStage());
        Path snapshotBase = duraBase.resolve(snapshotID);

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

            String tagDigest = getTagDigest(chronPackage.getBuildListenerWriter());
            log.info("Tag digest is {}", tagDigest);


            log.info("Pushing to chronopolis... ");
            // pushToChronopolis(chronPackage, location);

            log.info("Pushing to dpn...");
            // TODO: Also register with dpn if we need to
            registerDPNObject(chronPackage, tagDigest);
        }

        return RepeatStatus.FINISHED;
    }

    private String getTagDigest(Writer writer) {
        for (String s : writer.getFormattedTagDigests()) {
            // TODO: tagmanifest-${alg}.txt
            if (s.endsWith("tagmanifest-sha256.txt")) {
                return s;
            }
        }
        return null;
    }

    /**
     * Use the {@link IngestAPI} to register the bag with Chronopolis
     *
     * @param chronPackage - the bag to register
     * @param location - the relative location of the bag
     */
    private void pushToChronopolis(ChronPackage chronPackage, Path location) {
            IngestRequest chronRequest = new IngestRequest();
            chronRequest.setName(chronPackage.getSaveName());
            chronRequest.setDepositor(depositor);
            chronRequest.setLocation(location.toString()); // This is the relative path

            chronAPI.stageBag(chronRequest);
    }

    /**
     * Register the bag with the DPN REST API
     *
     * @param chronPackage
     */
    private void registerDPNObject(ChronPackage chronPackage, String tagDigest) {
        // We know the bag writer is a DpnBagWriter because IngestionType == DPN
        DpnBagWriter writer = (DpnBagWriter) chronPackage.getBuildListenerWriter();
        DPNBag bag = new DPNBag();

        // We know we have a dpn writer associated with it, so no fear

        // The two maps containing the dpn-info contents
        Map<String, String> dpnMetamap = writer.getDpnMetadata();
        Multimap<String, String> dpnMultimap = writer.getDpnMultimap();

        bag.setAdminNode(dpnMetamap.get(DpnBagWriter.FIRST_NODE_NAME))
                .setBagType('D')
                .setCreatedAt(new DateTime())
                .setFirstVersionUuid(dpnMetamap.get(DpnBagWriter.FIRST_VERSION_ID))
                .addFixity("sha256", tagDigest)
                // .setInterpretive()
                .setIngestNode(dpnMetamap.get(DpnBagWriter.FIRST_NODE_NAME))
                .setLocalId(dpnMetamap.get(DpnBagWriter.LOCAL_ID))
                // .setRights()
                .addReplicatingNode(dpnMetamap.get(DpnBagWriter.FIRST_NODE_NAME))
                .setSize(chronPackage.getTotalSize())
                .setUpdatedAt(new DateTime())
                .setUuid(dpnMetamap.get(DpnBagWriter.DPN_OBJECT_ID))
                .setVersion(Long.parseLong(dpnMetamap.get(DpnBagWriter.VERSION_NUMBER)));


        dpnService.createBag(bag);
    }

}
