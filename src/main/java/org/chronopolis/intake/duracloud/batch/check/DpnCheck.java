package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Ingest;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.cleaner.Cleaner;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.models.enums.BagStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class checks the DPN Registry in order to retrieve the completed
 * replications for a Bag (given by its receipt).
 * <p>
 * When all replications complete, there are a few operations left to do:
 * - push ingest records to the DPN Registry (records of when nodes were marked as having stored the Bag)
 * - remove the Bag from staging
 * - check the Chronopolis Ingest server to see if the staged data can be removed
 * <p>
 * Note that if any of these fail, the accumulator will be reset
 * <p>
 * Created by shake on 6/1/16.
 */
public class DpnCheck extends Checker {
    private final Logger log = LoggerFactory.getLogger(DpnCheck.class);

    private static final int EXPECTED_REPLICATIONS = 3;

    private final Events events;
    private final BalustradeBag bags;
    private final DepositorService depositors;
    private final Bicarbonate cleaningManager;
    private final IntakeSettings settings;

    public DpnCheck(BagData data,
                    List<BagReceipt> receipts,
                    BridgeAPI bridge,
                    BalustradeBag bags,
                    Events eventsAPI,
                    DepositorService depositors,
                    Bicarbonate cleaningManager,
                    IntakeSettings settings) {
        super(data, receipts, bridge);
        this.bags = bags;
        this.events = eventsAPI;
        this.depositors = depositors;
        this.cleaningManager = cleaningManager;
        this.settings = settings;
    }

    @Override
    protected void checkReceipts(BagReceipt receipt,
                                 BagData data,
                                 AtomicInteger accumulator,
                                 Map<String, ReplicationHistory> history) {
        String snapshot = data.snapshotId();
        log.info("[DPN Check] Checking {} for completion", snapshot);

        String uuid = receipt.getName();
        String depositor = data.depositor();
        Path dpn = Paths.get(depositor, uuid + ".tar");

        Call<Bag> call = bags.getBag(uuid);
        SimpleCallback<Bag> cb = new SimpleCallback<>();
        call.enqueue(cb);

        // not sure how much I like chaining 4 filters together but 
        List<String> replications = cb.getResponse()
                .map(bag -> isChronopolisPreserved(data, bag))
                // A good way to get the expected replications? Maybe in the future
                .filter(bag -> bag.getReplicatingNodes().size() == EXPECTED_REPLICATIONS)
                .filter(this::ingestRecordExists)
                .filter(bag -> cleaningManager.cleaner(dpn).call())
                .map(Bag::getReplicatingNodes).orElse(ImmutableList.of());

        for (String node : replications) {
            accumulator.incrementAndGet();
            ReplicationHistory h = history.getOrDefault(node,
                    new ReplicationHistory(snapshot, node, false));
            h.addReceipt(uuid);
            history.put(node, h);
        }
    }

    /**
     * Check if Chronopolis has replicated a DPN Bag
     *
     * @param data the BagData containing depositor information
     * @param bag  the DPN Bag
     * @return the DPN Bag, potentially updated if Chronopolis was added as a replicating node
     */
    private Bag isChronopolisPreserved(BagData data, Bag bag) {
        if (bag.getReplicatingNodes().contains(settings.getDpn().getUsername())) {
            return bag;
        }

        log.info("[DPN Check] {} querying for chronopolis replication", bag.getUuid());
        Call<org.chronopolis.rest.models.Bag> call =
                depositors.getDepositorBag(data.depositor(), bag.getUuid());
        SimpleCallback<org.chronopolis.rest.models.Bag> cb = new SimpleCallback<>();
        call.enqueue(cb);

        Cleaner cleaner = cleaningManager.forChronopolis(data.depositor(), bag.getUuid());
        return cb.getResponse()
                .filter(chronBag -> chronBag.getStatus() == BagStatus.PRESERVED)
                .flatMap(ignored -> addChronopolisReplication(bag))
                .filter(ignored -> cleaner.call())
                .orElse(bag);
    }

    /**
     * Update a Bag in DPN to include Chronopolis as a replicating node
     *
     * @param bag the Bag to update
     * @return the updated Bag
     */
    private Optional<Bag> addChronopolisReplication(Bag bag) {
        log.info("[DPN Check] {} adding chronopolis as replicating node", bag.getUuid());
        SimpleCallback<Bag> callback = new SimpleCallback<>();
        bag.setReplicatingNodes(new ImmutableList.Builder<String>()
                .addAll(bag.getReplicatingNodes())
                .add(settings.getDpn().getUsername()).build());
        Call<Bag> updateCall = bags.updateBag(bag.getUuid(), bag);
        updateCall.enqueue(callback);

        return callback.getResponse();
    }

    /**
     * Check if a ingest record exists for a bag, creating a record if it is not present
     *
     * @param bag the bag to retrieve the ingest record of
     * @return the
     */
    private Boolean ingestRecordExists(Bag bag) {
        log.info("[DPN Check] {} querying for ingest record", bag.getUuid());

        Call<Response<Ingest>> get = events.getIngests(ImmutableMap.of("bag", bag.getUuid()));
        SimpleCallback<Response<Ingest>> cb = new SimpleCallback<>();
        get.enqueue(cb);

        // A bit wonky but we need to filter then map or else we can fail to call
        // createIngestRecord when no record exists
        return cb.getResponse()
                .filter(response -> response.getCount() == 1)
                .map(response -> true)
                .orElseGet(() -> createIngestRecord(bag));
    }

    /**
     * Create an ingest record for a bag
     *
     * @param bag the bag to create an ingest record for
     * @return if the record was successfully created
     */
    private Boolean createIngestRecord(Bag bag) {
        log.info("[DPN Check] {} creating ingest record", bag.getUuid());

        Ingest record = new Ingest()
                .setIngestId(UUID.randomUUID().toString())
                .setBag(bag.getUuid())
                .setCreatedAt(ZonedDateTime.now())
                .setIngested(true)
                .setReplicatingNodes(bag.getReplicatingNodes());

        Call<Ingest> ingest = events.createIngest(record);
        SimpleCallback<Ingest> cb = new SimpleCallback<>();
        ingest.enqueue(cb);

        return cb.getResponse().isPresent();
    }

}
