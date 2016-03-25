package org.chronopolis.intake.duracloud.batch;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Node;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.IngestRequest;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Creates replications for both DPN and Chronopolis
 * <p/>
 * Ok so originally this was a Tasklet but since bags can be multipart,
 * we want to work on one bag (chunk) at a time.
 * <p/>
 * TODO: This does a lot (dpn {bag/replication}/chron). Might want to split it up.
 * TODO: Update new replication history flow
 * Created by shake on 11/12/15.
 */
public class ReplicationTasklet implements Runnable {

    /**
     * static factory class fo' testin'
     */
    static class ReaderFactory {
        DpnInfoReader reader(Path save, String name) throws IOException {
            TarArchiveInputStream is = new TarArchiveInputStream(Files.newInputStream(save));
            return DpnInfoReader.read(is, name);
        }
    }

    private final Logger log = LoggerFactory.getLogger(ReplicationTasklet.class);

    private final char DATA_BAG = 'D';
    private final String PARAM_PAGE_SIZE = "page_size";
    private final String PROTOCOL = "rsync";
    private final String ALGORITHM = "sha256";

    ReaderFactory readerFactory;
    IntakeSettings settings;
    String snapshot;
    String depositor;

    BagData data;
    List<BagReceipt> receipts;


    // Services to talk with both Chronopolis and DPN
    private LocalAPI dpn;
    private IngestAPI chronAPI;

    // And now featuring Duracloud
    private BridgeAPI bridge;

    public ReplicationTasklet(BagData data,
                              List<BagReceipt> receipts,
                              BridgeAPI bridge,
                              IngestAPI ingest,
                              LocalAPI dpn,
                              IntakeSettings settings) {
        this.data = data;
        this.receipts = receipts;
        this.bridge = bridge;
        this.chronAPI = ingest;
        this.dpn = dpn;
        this.settings = settings;
        this.readerFactory = new ReaderFactory();
    }

    @Override
    public void run() {
        boolean close = true;
        int requiredReplication = 3;

        // Http stuff
        BalustradeBag bags = dpn.getBagAPI();
        BalustradeTransfers transfers = dpn.getTransfersAPI();
        SimpleCallback<Bag> bagCallback;
        SimpleCallback<Response<Replication>> replicationCallback;

        // History stuff
        AlternateIds alternates = new AlternateIds();
        Map<String, ReplicationHistory> historyMap = new HashMap<>();

        snapshot = data.snapshotId();
        depositor = data.depositor();

        // Create a bag and replications for each receipt
        for (BagReceipt receipt : receipts) {
            log.info("Working on receipt {}", receipt.getName());
            String name = receipt.getName();
            alternates.addAlternateId(name);

            Path save = Paths.get(settings.getBagStage(),
                    depositor,
                    name + ".tar");


            // Get the bag and replications if they exist
            bagCallback = new SimpleCallback<>();
            replicationCallback = new SimpleCallback<>();
            Call<Bag> bagCall = bags.getBag(name);
            Call<Response<Replication>> replicationCall = transfers.getReplications(ImmutableMap.of("uuid", name));

            bagCall.enqueue(bagCallback);

            Optional<Bag> bagResponse = bagCallback.getResponse();
            boolean registered = true;


            // See if we need to create the bag (response is not 2xx so we assume so)
            if (!bagResponse.isPresent()) {
                close = false;
                log.info("Creating bag for {}", name);
                registered = registerDPNObject(save, receipt.getReceipt(), name);
            }

            replicationCall.enqueue(replicationCallback);
            Optional<Response<Replication>> replicationResponse = replicationCallback.getResponse();

            // Create/check our replications
            if (registered && replicationResponse.isPresent()) {
                Response<Replication> response = replicationResponse.get();

                // NOTE: The DPN API returns ALL replications if a bag uuid does not exist
                //       so going based of the size isn't always reliable, but should be fine
                if (response.getCount() == 0) {
                    close = false;
                    log.info("Creating replications for {}", name);
                    createDPNReplications(save, name);
                } else {
                    // Check the status of our replications
                    // It's easier to do it here as successful replications are returned to us
                    // in the bag object
                    Bag body = bagResponse.get();
                    List<String> replicatingNodes = body.getReplicatingNodes();

                    // because of this we check to make sure the name is the same
                    for (String node : replicatingNodes) {
                        ReplicationHistory history = historyMap.get(node);
                        if (history == null) {
                            history = new ReplicationHistory(snapshot, node, false);
                        }

                        history.addReceipt(name);
                        historyMap.put(node, history);
                    }

                    // Set our condition which we close upon
                    close &= replicatingNodes.size() == requiredReplication;
                }
            }

            pushToChronopolis(save, name);
        }

        if (close) {
            // only post replications when ALL have completed
            // not the best scenario, but it will avoid duplication of history entries for now
            // in the event these don't complete successfully, they should just be tried again
            log.info("Updating bridge with ReplicationHistory of snapshot {}", snapshot);
            for (ReplicationHistory history : historyMap.values()) {
                Call<HistorySummary> call = bridge.postHistory(snapshot, history);
                call.enqueue(new SimpleCallback<HistorySummary>());
            }

            log.info("Closing snapshot {}", snapshot);
            Call<SnapshotComplete> call = bridge.completeSnapshot(snapshot, alternates);
            call.enqueue(new SimpleCallback<SnapshotComplete>());
        }
    }


    /**
     * Steps:
     * Get DPN Nodes
     * Chose 2 random
     * Create replication requests
     */
    private void createDPNReplications(Path save, String name) {
        settings.getDuracloudSnapshotStage();
        String ourNode = dpn.getNode();
        int replications = 2;
        int count = 0;


        // 5 nodes -> page size of 5
        List<String> nodes;
        retrofit2.Response<Node> response = null;
        Call<Node> call = dpn.getNodeAPI().getNode(settings.getNode());
        try {
            response = call.execute();
        } catch (IOException e) {
            log.error("Error communicating with server", e);
        }

        if (response != null && response.isSuccess()) {
            Node myNode = response.body();
            nodes = myNode.getReplicateTo();
        } else {
            // error communicating, don't make an attempt to create replications
            if (response != null) {
                log.error("Error in response: {} - {}", response.code(), response.message());
            } else {
                log.error("Error in response: null response");
            }
            nodes = ImmutableList.of();
            replications = 0;
        }

        Random r = new Random();
        Set<Integer> seen = new HashSet<>();
        // this can get stuck if we don't replicate to at least 2 nodes
        while (count < replications) {
            int index = r.nextInt(nodes.size());
            String node = nodes.get(index);

            if (seen.contains(index)) {
                continue;
            }

            seen.add(index);
            log.debug("Adding replication for {}", node);
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

            Call<Replication> replCall = dpn.getTransfersAPI().createReplication(repl);/*, new Callback<Void>() {
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

            try {
                retrofit2.Response<Replication> replResponse = replCall.execute();
                if (replResponse.isSuccess()) {
                    ++count;
                    // nodes.remove(index);
                }
            } catch (IOException e) {
                log.error("", e);
            }
        }
    }

    /**
     * Use the {@link IngestAPI} to register the bag with Chronopolis
     *
     * @param location - the relative location of the bag
     * @param name
     */
    private void pushToChronopolis(Path location, String name) {
        IngestRequest chronRequest = new IngestRequest();
        chronRequest.setRequiredReplications(1);
        chronRequest.setName(name);
        chronRequest.setDepositor(depositor);
        chronRequest.setLocation(location.toString()); // This is the relative path

        chronRequest.setReplicatingNodes(
                ImmutableList.of(settings.getChronReplicationNodes()));

        Call<org.chronopolis.rest.entities.Bag> stageCall = chronAPI.stageBag(chronRequest);
        try {
            retrofit2.Response<org.chronopolis.rest.entities.Bag> response = stageCall.execute();

        } catch (IOException e) {
            log.error("Unable to stage bag with chronopolis", e);
        }
    }

    /**
     * Register the bag with the DPN REST API
     *
     * @param save    - the path of the serialized bag
     * @param receipt - the receipt of the serialized bag
     * @param name
     */
    private boolean registerDPNObject(Path save, final String receipt, String name) {
        DpnInfoReader reader;
        try {
            reader = readerFactory.reader(save, name);
        } catch (IOException e) {
            log.error("Unable to read dpn-info from bag, abortin'", e);
            return false;
        }

        // dpn bag
        Bag bag = new Bag();

        // TODO: No magic
        bag.setAdminNode("chron")
                .setUuid(name)
                .setBagType(DATA_BAG)
                .setCreatedAt(new DateTime())
                .setUpdatedAt(new DateTime())
                .setSize(save.toFile().length()) // idk
                .setLocalId(reader.getLocalId())
                .setRights(reader.getRightsIds())
                .setMember(data.member())                                // must be a valid uuid
                .setVersion(reader.getVersionNumber())
                .setIngestNode(reader.getIngestNodeName())
                .setInterpretive(reader.getInterpretiveIds())
                .setFixities(ImmutableMap.of("sha256", receipt))         // sha256 digest
                .setFirstVersionUuid(reader.getFirstVersionUUID())       // uuid
                .setReplicatingNodes(ImmutableList.of("chron"));      // chron

        Call<Bag> call = dpn.getBagAPI().createBag(bag);/*, new retrofit.Callback<Bag>() {
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
        });*/
        try {
            retrofit2.Response<Bag> response = call.execute();
            if (response.isSuccess()) {
                log.info("Success registering bag {}", bag.getUuid());
            } else {
                log.info("Failure registering bag {}. Reason: {}", bag.getUuid(), response.message());
                return false;
            }
        } catch (IOException e) {
            log.info("Failure communicating with server", e);
            return false;
        }

        return true;
    }
}
