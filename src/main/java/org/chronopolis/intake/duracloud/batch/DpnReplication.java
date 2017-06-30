package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Chron;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

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
            try (TarArchiveInputStream is = new TarArchiveInputStream(Files.newInputStream(save))) {
                return DpnInfoReader.read(is, name);
            }
        }
    }

    private final Logger log = LoggerFactory.getLogger(DpnReplication.class);

    private final char DATA_BAG = 'D';
    private final String PARAM_PAGE_SIZE = "page_size";
    private final String PROTOCOL = "rsync";
    private final String ALGORITHM = "sha256";

    private ReaderFactory readerFactory;
    private IntakeSettings settings;
    private String snapshot;
    private String depositor;

    private BagData data;
    private List<Weight> weights;
    private List<BagReceipt> receipts;

    // Services to talk with both Chronopolis and DPN
    private LocalAPI dpn;

    public DpnReplication(BagData data,
                          List<BagReceipt> receipts,
                          List<Weight> weights,
                          LocalAPI dpn,
                          IntakeSettings settings) {
        this.data = data;
        this.receipts = receipts;
        this.weights = weights;
        this.dpn = dpn;
        this.settings = settings;
        this.readerFactory = new ReaderFactory();
    }

    @Override
    public void run() {
        snapshot = data.snapshotId();
        depositor = data.depositor();

        // Create a bag and replications for each receipt
        receipts.stream()
                .map(this::getBag) // get our bag (1) or create (1b)
                .forEach(o -> o.ifPresent(this::replicate)); // create replications (2, 2a, 2b)
    }

    /**
     * Create replications for a bag
     *
     * Search for replications already created
     *  -> if none exist, create both
     *  -> if one exists, create the missing
     *
     * @param bag the bag to replicate
     */
    private void replicate(Bag bag) {
        BalustradeTransfers transfers = dpn.getTransfersAPI();

        Call<Response<Replication>> call = transfers.getReplications(ImmutableMap.of("bag", bag.getUuid()));
        SimpleCallback<Response<Replication>> rcb = new SimpleCallback<>();
        call.enqueue(rcb);

        Optional<Response<Replication>> ongoing = rcb.getResponse();
        ongoing.ifPresent(response -> {
            // We'll only deal with the two easy cases for now
            // I think this can be updated to be a bit smoother but the pieces seem to be in place at least
            if (response.getCount() == 0) {
                pushReplication(bag, (weight) -> true);
            } else if (response.getCount() == 1) {
                Replication exists = response.getResults().get(0);
                pushReplication(bag, (weight) -> !weight.getNode().equalsIgnoreCase(exists.getToNode()));
            }
        });
    }

    private void pushReplication(Bag bag, Predicate<Weight> predicate) {
        weights.stream()
                .limit(2)
                .filter(predicate)
                .map(weight -> call(bag, weight.getNode()))
                .forEach(call -> call.enqueue(new SimpleCallback<>()));
    }

    private Call<Replication> call(Bag bag, String to) {
        Chron chron = settings.getChron();
        BalustradeTransfers transfers = dpn.getTransfersAPI();

        Path save = Paths.get(chron.getBags(), depositor, bag.getUuid() + ".tar");
        String ourNode = dpn.getNode();

        Replication replication = new Replication();
        replication.setCreatedAt(ZonedDateTime.now());
        replication.setUpdatedAt(ZonedDateTime.now());
        replication.setReplicationId(UUID.randomUUID().toString());
        replication.setFromNode(ourNode);
        replication.setToNode(to);
        replication.setLink(to + "@" + settings.getDpnReplicationServer() + ":" + save.toString());
        replication.setProtocol(PROTOCOL);
        replication.setStored(false);
        replication.setStoreRequested(false);
        replication.setCancelled(false);
        replication.setBag(bag.getUuid());
        replication.setFixityAlgorithm(ALGORITHM);

        return transfers.createReplication(replication);
    }

    // There are 3 cases, therefore 3 things can happen:
    // 1: Error communicating with server - IOException; empty()
    // 2: Bag does not exist (404)
    // 2a: Attempt to create bag - return of(Bag) on success, empty() on fail
    // 3: Bag exists (200) - of(bag)
    private Optional<Bag> getBag(BagReceipt receipt) {
        Optional<Bag> value = Optional.empty();

        log.debug("Seeing if bag is already registered for receipt {}", receipt.getName());
        BalustradeBag bags = dpn.getBagAPI();
        Call<Bag> bagCall = bags.getBag(receipt.getName());
        retrofit2.Response<Bag> response;
        try {
            response = bagCall.execute();
            if (response.isSuccessful()) {
                value = Optional.of(response.body());
            } else {
                value = createBag(receipt);
            }
        } catch (IOException e) {
            log.warn("Error communicating with registry", e);
        }

        return value;
    }

    // TODO: Create the bag and digest separate from one another?
    private Optional<Bag> createBag(BagReceipt receipt) {
        log.info("Creating bag for receipt {}", receipt.getName());

        Chron chron = settings.getChron();
        String name = receipt.getName();
        Path save = Paths.get(chron.getBags(), depositor, name + ".tar");

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
        //       Also the ingest node name is a bit iffy at the moment
        //       as we used to pull from the reader but now store the
        //       full chronopolis name there. For now we can use the setting.
        bag.setAdminNode("chron")
                .setUuid(name)
                .setBagType(DATA_BAG)
                .setMember(data.member())
                .setCreatedAt(ZonedDateTime.now())
                .setUpdatedAt(ZonedDateTime.now())
                // Size of the tarball, should be good enough
                // could also use the size from bag-info.txt
                .setSize(save.toFile().length())
                .setLocalId(reader.getLocalId())
                .setRights(reader.getRightsIds())
                .setVersion(reader.getVersionNumber())
                // .setIngestNode(reader.getIngestNodeName())
                .setIngestNode(settings.getDpn().getUsername())
                .setInterpretive(reader.getInterpretiveIds())
                .setFirstVersionUuid(reader.getFirstVersionUUID())
                .setReplicatingNodes(ImmutableList.of("chron"));

        // MessageDigest
        Digest bagDigest = new Digest();
        bagDigest.setAlgorithm("sha256");
        bagDigest.setBag(bag.getUuid());
        bagDigest.setValue(receipt.getReceipt());
        bagDigest.setNode("chron");
        bagDigest.setCreatedAt(ZonedDateTime.now());

        // TODO: Maybe look for a way to clean this up a bit
        Call<Bag> call = dpn.getBagAPI().createBag(bag);
        Call<Digest> digestCall = dpn.getBagAPI().createDigest(name, bagDigest);
        try {
            retrofit2.Response<Bag> response = call.execute();
            retrofit2.Response<Digest> digest = digestCall.execute();
            if (response.isSuccessful() && digest.isSuccessful()) {
                log.info("Success registering bag {}", bag.getUuid());
                optional = Optional.of(bag);
            } else {
                retrofit2.Response failure = response.isSuccessful() ? digest : response;
                log.info("Failure registering bag {} - {}: {}", bag.getUuid(),
                        failure.message(),
                        failure.errorBody().string());
            }
        } catch (IOException e) {
            log.info("Failure communicating with server", e);
        }

        return optional;
    }

}
