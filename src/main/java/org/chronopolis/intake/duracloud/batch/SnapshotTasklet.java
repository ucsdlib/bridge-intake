package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Node;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.ingest.bagger.IngestionType;
import org.chronopolis.ingest.pkg.ChronPackage;
import org.chronopolis.ingest.pkg.DpnBagWriter;
import org.chronopolis.ingest.pkg.ManifestBuilder;
import org.chronopolis.ingest.pkg.Unit;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * {@link Tasklet} which processes a snapshot from Duraspace. We bag the snapshot and
 * push it to both Chronopolis and DPN.
 *
 * Created by shake on 9/19/14.
 */
public class SnapshotTasklet implements Tasklet {
    private final Logger log = LoggerFactory.getLogger(SnapshotTasklet.class);

    private final char DATA_BAG = 'D';
    private final String PARAM_PAGE_SIZE = "page_size";
    private final String PROTOCOL = "rsync";
    private final String ALGORITHM = "sha256";

    private String snapshotID;
    private String collectionName;
    private String depositor;
    private IntakeSettings settings;

    // Services to talk with both Chronopolis and DPN
    private IngestAPI chronAPI;
    private LocalAPI dpn;

    public SnapshotTasklet(String snapshotID,
                           String collectionName,
                           String depositor,
                           IntakeSettings settings,
                           IngestAPI chronAPI,
                           LocalAPI dpn) {
        this.snapshotID = snapshotID;
        this.collectionName = collectionName;
        this.depositor = depositor;
        this.settings = settings;

        this.chronAPI = chronAPI;
        this.dpn = dpn;
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

            // String tagDigest = getTagDigest(chronPackage.getBuildListenerWriter());
            String receipt = com.google.common.io.Files.hash(saveFile.toFile(),
                                                             Hashing.sha256())
                                                       .toString();
            log.info("Digest is {}", receipt);


            // TODO: Make this configurable (load based on profile - can have an interface for both and a null/real impl)
            // TODO: Don't rely on these to succeed, we may need to try multiple times
            //       Maybe by v1.2 have a db backend so that we can persist things
            if (settings.pushChronopolis()) {
                log.info("Registering with chronopolis... ");
                pushToChronopolis(chronPackage, location);
            }

            if (settings.pushDPN()) {
                log.info("Registering with dpn...");
                registerDPNObject(chronPackage, receipt);

                log.info("Creating replications for dpn...");
                createDPNReplications(chronPackage);
            }
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * Steps:
     * Get DPN Nodes
     * Chose 2 random
     * Create replication requests
     *
     * @param chronPackage the package to create replications for
     */
    private void createDPNReplications(ChronPackage chronPackage) {
        // TODO: Remove magic values
        //       - hostname : should be set in external properties
        Path save = Paths.get(settings.getBagStage(),
                chronPackage.getDepositor(),
                chronPackage.getSaveName() + ".tar");
        settings.getDuracloudSnapshotStage();
        String ourNode = dpn.getNode();
        int replications = 2;
        int count = 0;


        // 5 nodes -> page size of 5
        Node myNode = dpn.getNodeAPI().getNode(settings.getNode());
        List<String> nodes = myNode.getReplicateTo();

        Random r = new Random();
        Set<Integer> seen = new HashSet<>();
        while (count < replications) {
            int index = r.nextInt(nodes.size());
            String node = nodes.get(index);

            if (seen.contains(index)) {
                continue;
            }

            seen.add(index);
            Replication repl = new Replication();
            repl.setStatus(Replication.Status.REQUESTED);
            repl.setCreatedAt(DateTime.now());
            repl.setUpdatedAt(DateTime.now());
            repl.setReplicationId(UUID.randomUUID().toString());
            repl.setFromNode(ourNode);
            repl.setToNode(node);
            repl.setLink(node + "@" + settings.getDpnReplicationServer() + ":" + save.toString());
            repl.setProtocol(PROTOCOL);
            repl.setUuid(chronPackage.getSaveName());
            repl.setFixityAlgorithm(ALGORITHM);

            dpn.getTransfersAPI().createReplication(repl); /*, new Callback<Void>() {
                @Override
                public void success(Void aVoid, retrofit.client.Response response) {
                    log.info("Success!!");
                }

                @Override
                public void failure(RetrofitError retrofitError) {
                    if (retrofitError.getResponse() != null) {
                        log.info("Failure! {} {}", retrofitError.getResponse().getStatus(), retrofitError.getResponse().getReason());
                    } else {
                        log.info("Failure! {} {}", retrofitError.getUrl(), retrofitError.getMessage());
                    }

                }
            });
            */
            ++count;
        }


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

        chronRequest.setReplicatingNodes(
                ImmutableList.of(settings.getChronReplicationNodes()));

        chronAPI.stageBag(chronRequest);
    }

    /**
     * Register the bag with the DPN REST API
     *
     * @param chronPackage
     */
    private void registerDPNObject(ChronPackage chronPackage, final String receipt) {
        // We know the bag writer is a DpnBagWriter because IngestionType == DPN
        DpnBagWriter writer = (DpnBagWriter) chronPackage.getBuildListenerWriter();

        // dpn bag
        Bag bag = new Bag();

        // We know we have a dpn writer associated with it, so no fear

        // The two maps containing the dpn-info contents
        Map<String, String> dpnMetamap = writer.getDpnMetadata();
        Multimap<String, String> dpnMultimap = writer.getDpnMultimap();
        String uuid = dpnMetamap.get(DpnBagWriter.DPN_OBJECT_ID);

        bag.setAdminNode(dpnMetamap.get(DpnBagWriter.INGEST_NODE_NAME))
                .setBagType(DATA_BAG)
                .setCreatedAt(new DateTime())
                .setFirstVersionUuid(dpnMetamap.get(DpnBagWriter.FIRST_VERSION_ID))
                .setFixities(ImmutableMap.of(chronPackage.getBagFormattedDigest(), receipt)) // sha256 digest
                .setInterpretive(ImmutableList.<String>of())
                .setIngestNode(dpnMetamap.get(DpnBagWriter.INGEST_NODE_NAME))
                .setLocalId(dpnMetamap.get(DpnBagWriter.LOCAL_ID))
                .setRights(ImmutableList.<String>of())
                .setReplicatingNodes(ImmutableList.of(dpnMetamap.get(DpnBagWriter.INGEST_NODE_NAME)))
                .setSize(chronPackage.getTotalSize())
                .setUpdatedAt(new DateTime())
                .setUuid(uuid)
                .setMember(settings.getMemberUUID()) // must be a valid uuid
                .setVersion(Long.parseLong(dpnMetamap.get(DpnBagWriter.VERSION_NUMBER)));

        dpn.getBagAPI().createBag(bag); /*, new retrofit.Callback<Bag>() {
            @Override
            public void success(Bag bag, retrofit.client.Response response) {
                log.info("Success! ");
            }

            @Override
            public void failure(RetrofitError retrofitError) {
                if (retrofitError.getResponse() != null) {
                    log.info("Failure! {} {}", retrofitError.getResponse().getStatus(), retrofitError.getResponse().getReason());
                } else {
                    log.info("Failure! {} {}", retrofitError.getUrl(), retrofitError.getMessage());
                }

            }
        });
        */
    }

    private void getMember() {
    }

}
