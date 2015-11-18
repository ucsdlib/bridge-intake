package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Node;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.DpnInfoReader;
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
import retrofit.RetrofitError;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Creates replications for both DPN and Chronopolis
 *
 * Created by shake on 11/12/15.
 */
public class ReplicationTasklet implements Tasklet {
    private final Logger log = LoggerFactory.getLogger(ReplicationTasklet.class);

    private final char DATA_BAG = 'D';
    private final String PARAM_PAGE_SIZE = "page_size";
    private final String PROTOCOL = "rsync";
    private final String ALGORITHM = "sha256";

    IntakeSettings settings;
    String name;
    String depositor;
    String receipt;

    // Services to talk with both Chronopolis and DPN
    private LocalAPI dpn;
    private IngestAPI chronAPI;

    public ReplicationTasklet(IntakeSettings settings, String name, String depositor, String receipt, IngestAPI ingest, LocalAPI dpn) {
        this.settings = settings;
        this.name = name;
        this.depositor = depositor;
        this.receipt = receipt;
        this.chronAPI = ingest;
        this.dpn = dpn;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Path save = Paths.get(settings.getBagStage(),
                depositor,
                name + ".tar");

        registerDPNObject(save, receipt);
        Response<Replication> uuid = dpn.getTransfersAPI().getReplications(ImmutableMap.of("uuid", name));
        if (uuid.getCount() == 0) {
            createDPNReplications(save);
        } else {
            checkDPNReplications(save);
        }
        pushToChronopolis(save);

        return RepeatStatus.FINISHED;
    }

    private void checkDPNReplications(Path save) {

    }


    /**
     * Steps:
     * Get DPN Nodes
     * Chose 2 random
     * Create replication requests
     *
     */
    private void createDPNReplications(Path save) {
        settings.getDuracloudSnapshotStage();
        String ourNode = dpn.getNode();
        int replications = 2;
        int count = 0;


        // 5 nodes -> page size of 5
        // TODO: DPN Namespace
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
            repl.setUuid(name);
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
     * @param location - the relative location of the bag
     */
    private void pushToChronopolis(Path location) {
        IngestRequest chronRequest = new IngestRequest();
        chronRequest.setName(name);
        chronRequest.setDepositor(depositor);
        chronRequest.setLocation(location.toString()); // This is the relative path

        chronRequest.setReplicatingNodes(
                ImmutableList.of(settings.getChronReplicationNodes()));

        chronAPI.stageBag(chronRequest);
    }

    /**
     * Register the bag with the DPN REST API
     *
     * @param save - the path of the serialized bag
     * @param receipt - the receipt of the serialized bag
     */
    private boolean registerDPNObject(Path save, final String receipt) {
        DpnInfoReader reader;
        try {
            TarArchiveInputStream is = new TarArchiveInputStream(Files.newInputStream(save));
            reader = DpnInfoReader.read(is, name);
        } catch (IOException e) {
            log.error("Unable to read dpn-info from bag, abortin'", e);
            return false;
        }

        // dpn bag
        Bag bag = new Bag();

        // We know we have a dpn writer associated with it, so no fear

        // The two maps containing the dpn-info contents
        Multimap<DpnInfoReader.Tag, String> info = reader.getTags();

        // TODO: No magic
        bag.setAdminNode("chron")
                .setUuid(name)
                .setBagType(DATA_BAG)
                .setCreatedAt(new DateTime())
                .setUpdatedAt(new DateTime())
                .setSize(save.toFile().length()) // idk
                .setLocalId(reader.getLocalId())
                .setRights(reader.getRightsIds())
                .setMember(settings.getMemberUUID())                     // must be a valid uuid
                .setVersion(reader.getVersionNumber())
                .setIngestNode(reader.getIngestNodeName())
                .setInterpretive(reader.getInterpretiveIds())
                .setFixities(ImmutableMap.of("sha256", receipt))         // sha256 digest
                .setFirstVersionUuid(reader.getFirstVersionUUID())       // uuid
                .setReplicatingNodes(ImmutableList.<String>of("chron")); // chron

        dpn.getBagAPI().createBag(bag); new retrofit.Callback<Bag>() {
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
        };

        return true;
    }
}
