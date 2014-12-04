package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.sun.org.apache.xpath.internal.operations.Mult;
import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.amqp.RoutingKey;
import org.chronopolis.common.digest.Digest;
import org.chronopolis.common.dpn.DPNService;
import org.chronopolis.common.dpn.RegistryItemModel;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.ingest.pkg.DpnBagWriter;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.ingest.pkg.Unit;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.messaging.factory.MessageFactory;
import org.chronopolis.messaging.pkg.PackageReadyMessage;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import retrofit.Callback;
import retrofit.RetrofitError;
import retrofit.client.Response;

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
    private ChronProducer producer;
    private MessageFactory messageFactory;
    private DPNService dpnService;


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
        this.producer = producer;
        this.messageFactory = messageFactory;
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
        builder.setMaxSize(100, Unit.GIGABYTE);
        builder.setName(collectionName);
        builder.setIngestionType(IngestionType.DPN);

        // And bag (with a sha256 manifest)
        builder.loadManifest(Files.newBufferedReader(
                snapshotBase.resolve(settings.getDuracloudManifest()),
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
            Path location = Paths.get(settings.getBagStage())
                                 .relativize(saveFile);

            log.info("Save file {}; Save Name {}", saveFile, chronPackage.getSaveName());

            PackageReadyMessage packageReadyMessage = messageFactory.packageReadyMessage(
                    depositor,
                    digest,
                    location.toString(),        // This is the relative path
                    chronPackage.getSaveName(), // (ingest may have a different mount)
                    size
            );

            producer.send(packageReadyMessage, RoutingKey.INGEST_BROADCAST.asRoute());

            // TODO: Also register with dpn if we need to
            registerDPNObject(chronPackage, location.toString());

        }

        return RepeatStatus.FINISHED;
    }

    private void registerDPNObject(ChronPackage chronPackage, String location) {
        // We know the bag writer is a DpnBagWriter because IngestionType == DPN
        DpnBagWriter writer = (DpnBagWriter) chronPackage.getBuildListenerWriter();
        RegistryItemModel model = new RegistryItemModel();

        // We know we have a dpn writer associated with it, so no fear
        DateTimeFormatter formatter = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC();
        String now = formatter.print(new DateTime());

        // The two maps containing the dpn-info contents
        Map<String, String> dpnMetamap = writer.getDpnMetadata();
        Multimap<String, String> dpnMultimap = writer.getDpnMultimap();

        model.setBagSize(chronPackage.getTotalSize());
        model.setBrighteningObjectId(Sets.newHashSet(dpnMultimap.get(DpnBagWriter.BRIGHTENING_OBJECT_ID)));
        model.setCreationDate(now);
        model.setDpnObjectId(dpnMetamap.get(DpnBagWriter.DPN_OBJECT_ID));
        model.setFirstNodeName(dpnMetamap.get(DpnBagWriter.FIRST_NODE_NAME));
        model.setFirstVersionId(dpnMetamap.get(DpnBagWriter.FIRST_VERSION_ID));
        // model.setForwardVersionObjectId();
        model.setFixityAlgorithm("sha256");
        // model.setFixityValue();
        model.setLocalId(dpnMetamap.get(DpnBagWriter.LOCAL_ID));

        // This is the relative location of the package, so the same thing we
        // used for the amqp message
        model.setLocation(location);

        model.setLastFixityDate(now);
        model.setLastModifiedDate(now);
        model.setObjectType(dpnMetamap.get(DpnBagWriter.OBJECT_TYPE));
        model.setPreviousVersionObjectId(dpnMetamap.get(DpnBagWriter.PREVIOUS_VERSION_ID));
        model.addReplicatingNode(dpnMetamap.get(DpnBagWriter.FIRST_NODE_NAME));
        model.setRightsObjectId(Sets.newHashSet(dpnMultimap.get(DpnBagWriter.RIGHTS_OBJECT_ID)));
        model.setState("staged");
        model.setVersionNumber(Long.parseLong(dpnMetamap.get(DpnBagWriter.VERSION_NUMBER)));


        dpnService.putRegistryItem(model, new Callback<Void>() {
            @Override
            public void success(final Void aVoid, final Response response) {
                log.info("Successfully registered registry item");
            }

            @Override
            public void failure(final RetrofitError error) {
                log.error("Failed to register registry item", error);
            }
        });
    }

}
