package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
public class DpnReplication implements Runnable {

    /**
     * static factory class fo' testin'
     */
    static class ReaderFactory {
        DpnInfoReader reader(Path save, String name) throws IOException {
            TarArchiveInputStream is = new TarArchiveInputStream(Files.newInputStream(save));
            return DpnInfoReader.read(is, name);
        }
    }

    private final Logger log = LoggerFactory.getLogger(DpnReplication.class);

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

    // And now featuring Duracloud
    // private BridgeAPI bridge;

    public DpnReplication(BagData data,
                          List<BagReceipt> receipts,
                          LocalAPI dpn,
                          IntakeSettings settings) {
        this.data = data;
        this.receipts = receipts;
        this.dpn = dpn;
        this.settings = settings;
        this.readerFactory = new ReaderFactory();
    }

    @Override
    public void run() {
        snapshot = data.snapshotId();
        depositor = data.depositor();

        /**
         * Creating/Updating bags and replications
         * Feels a bit weird to have these all in one but that's ok for now
         *
         * 1. Bags
         * 1a. Get bags
         * 1b. Create any bag which doesn't exist
         * 2. Replications
         * 2a. Permute nodes
         * 2b. Search for replications
         * 2c. If replications don't exist, create them
         * 3. History (handled in dpn check)
         * 3a. Pull replicating nodes out of the bag object
         * 3b. Add replicating node to history map
         * 3c. If all bags are successful, push history to the bridge
         *
         */

        // Replicating nodes
        Random ran = new Random();
        List<String> nodes = loadNode();
        List<List<String>> permutations = ImmutableList.copyOf(Collections2.permutations(nodes));
        int pSize = permutations.size();

        Map<String, ReplicationHistory> historyMap = new HashMap<>();
        AtomicInteger accumulator = new AtomicInteger(0);

        // It seems like this is n^2, we could do a filter, then map, but... well, we'll see
        // Create a bag and replications for each receipt
        receipts.stream()
                .map(this::getBag) // get our bag (1)
                .filter(Optional::isPresent) // annoying but w.e.
                .map(Optional::get)
                .map(b -> createReplication(b, permutations.get(ran.nextInt(pSize)))) // create replications (2 + 2a)
                .forEach(b -> b.getReplicatingNodes().forEach(n -> { // update history (3)
                    ReplicationHistory history = historyMap.getOrDefault(n, new ReplicationHistory(snapshot, n, false));
                    history.addReceipt(b.getUuid());
                    historyMap.put(n, history);
                    accumulator.incrementAndGet();
                }));
    }

    private Bag createReplication(Bag bag, List<String> permutation) {
        Path save = Paths.get(settings.getBagStage(), depositor, bag.getUuid() + ".tar");
        String ourNode = dpn.getNode();
        int replications = 2;
        int count = 0;

        BalustradeTransfers transfers = dpn.getTransfersAPI();

        // blehhh
        List<String> nodes = Lists.newArrayList(permutation);

        // Short circuit if necessary
        log.info("Checking replications for bag {}", bag.getUuid());
        SimpleCallback<Response<Replication>> rcb = new SimpleCallback<>();
        Call<Response<Replication>> ongoing = transfers.getReplications(ImmutableMap.of("uuid", bag.getUuid()));
        ongoing.enqueue(rcb);
        com.google.common.base.Optional<Response<Replication>> ongoingResponse = rcb.getResponse();
        if (ongoingResponse.isPresent()) {
            count += ongoingResponse.get().getCount();
        }

        log.info("count: {}", count);
        // this can get stuck if we don't replicate to at least 2 nodes
        // ok lets create some scenarios
        // p = count < replications
        // q = !nodes.isEmpty
        // p && q => true (less replications and nodes to go)
        // !p && q => false (created replications and nodes to go)
        // p && !q => false (not enough replications and no nodes)
        // !p && !q => false (created replications and no nodes)
        while (count < replications && !nodes.isEmpty()) {
            String node = nodes.remove(0);
            log.info("Creating replications for bag {} to {}", bag.getUuid(), node);

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
            repl.setUuid(bag.getUuid());
            repl.setFixityAlgorithm(ALGORITHM);

            Call<Replication> replCall = transfers.createReplication(repl);
            try {
                retrofit2.Response<Replication> replResponse = replCall.execute();
                if (replResponse.isSuccess()) {
                    ++count;
                    // nodes.remove(index);
                }
            } catch (IOException e) {
                log.error("Could not create replication", e);
            }
        }

        return bag;
    }

    private List<String> loadNode() {
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
        }

        return nodes;
    }

    // There are 3 cases, therefore 3 things can happen:
    // 1: Error communicating with server - IOException; empty()
    // 2: Bag does not exist (404)
    // 2a: Attempt to create bag - return of(Bag) on success, empty() on fail
    // 3: Bag exists (200) - of(bag)
    private Optional<Bag> getBag(BagReceipt receipt) {
        log.info("Seeing if bag is already registered for receipt {}", receipt.getName());
        BalustradeBag bags = dpn.getBagAPI();
        Call<Bag> bagCall = bags.getBag(receipt.getName());
        retrofit2.Response<Bag> response = null;
        try {
            response = bagCall.execute();
        } catch (IOException e) {
            // TODO: Figure this out
            response = retrofit2.Response.error(500, ResponseBody.create(MediaType.parse("text/plain"), "empty"));
        }
        return response.isSuccess() ? Optional.of(response.body()) : createBag(receipt);
    }

    private Optional<Bag> createBag(BagReceipt receipt) {
        log.info("Creating bag for receipt {}", receipt.getName());

        String name = receipt.getName();
        Path save = Paths.get(settings.getBagStage(), depositor, name + ".tar");

        Optional<Bag> optional = Optional.empty();

        DpnInfoReader reader;
        try {
            reader = readerFactory.reader(save, name);
        } catch (IOException e) {
            log.error("Unable to read dpn-info from bag, abortin'", e);
            return Optional.empty();
        }

        // dpn bag
        Bag bag = new Bag();

        // TODO: No magic (sha256/admin node/replicating node)
        bag.setAdminNode("chron")
                .setUuid(name)
                .setBagType(DATA_BAG)
                .setMember(data.member())
                .setCreatedAt(new DateTime())
                .setUpdatedAt(new DateTime())
                // Size of the tarball, should be good enough
                .setSize(save.toFile().length())
                .setLocalId(reader.getLocalId())
                .setRights(reader.getRightsIds())
                .setVersion(reader.getVersionNumber())
                .setIngestNode(reader.getIngestNodeName())
                .setInterpretive(reader.getInterpretiveIds())
                .setFirstVersionUuid(reader.getFirstVersionUUID())
                // sha256 digest from our receipt
                .setFixities(ImmutableMap.of("sha256", receipt.getReceipt()))
                .setReplicatingNodes(ImmutableList.of("chron"));

        // TODO: Maybe look for a way to clean this up a bit
        Call<Bag> call = dpn.getBagAPI().createBag(bag);
        try {
            retrofit2.Response<Bag> response = call.execute();
            if (response.isSuccess()) {
                log.info("Success registering bag {}", bag.getUuid());
                optional = Optional.of(bag);
            } else {
                log.info("Failure registering bag {}. Reason: {}", bag.getUuid(), response.message());
            }
        } catch (IOException e) {
            log.info("Failure communicating with server", e);
        }

        return optional;
    }

}
