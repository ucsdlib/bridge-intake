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
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
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

    private final Events events;
    private final BalustradeBag bags;
    private final Bicarbonate cleaningManager;

    public DpnCheck(BagData data,
                    List<BagReceipt> receipts,
                    BridgeAPI bridge,
                    BalustradeBag bags,
                    Events eventsAPI,
                    Bicarbonate cleaningManager) {
        super(data, receipts, bridge);
        this.bags = bags;
        this.events = eventsAPI;
        this.cleaningManager = cleaningManager;
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
                .filter(bag -> bag.getReplicatingNodes().size() == 3)
                .filter(this::ingestRecordExists)
                .filter(bag -> cleaningManager.cleaner(dpn).call())
                .filter(bag -> cleaningManager.forChronopolis(depositor, uuid).call())
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
