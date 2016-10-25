package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableMap;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Ingest;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class validates BagReceipts against the DPN registry
 * <p>
 * Created by shake on 6/1/16.
 */
public class DpnCheck extends Checker {
    private final Logger log = LoggerFactory.getLogger(DpnCheck.class);

    private BalustradeBag bags;
    private Events events;

    public DpnCheck(BagData data, List<BagReceipt> receipts, BridgeAPI bridge, LocalAPI dpn) {
        super(data, receipts, bridge);
        this.bags = dpn.getBagAPI();
        this.events = dpn.getEventsAPI();
    }

    @Override
    protected void checkReceipts(BagReceipt receipt, BagData data, AtomicInteger accumulator, Map<String, ReplicationHistory> history) {
        AtomicInteger localAccumulator = new AtomicInteger(0);
        String snapshot = data.snapshotId();
        Call<Bag> call = bags.getBag(receipt.getName());
        try {
            retrofit2.Response<Bag> response = call.execute();
            if (response.isSuccessful()) {
                Bag bag = response.body();

                // Once again, might revisit this
                bag.getReplicatingNodes().forEach(n -> {
                    localAccumulator.incrementAndGet();
                    accumulator.incrementAndGet();
                    ReplicationHistory h = history.getOrDefault(n, new ReplicationHistory(snapshot, n, false));
                    h.addReceipt(bag.getUuid());
                    history.put(n, h);
                });

                if (localAccumulator.get() == 3) {
                    createIngestRecord(bag);
                }
            }
        } catch (IOException e) {
            // denote an error
            log.error("", e);
            accumulator.set(-1);
        }
    }

    /**
     * Create an ingest record for a bag
     *
     * @param bag the bag to create an ingest record for
     * @throws IOException if there is a problem communicating with the dpn server
     */
    private void createIngestRecord(Bag bag) throws IOException {
        // short circuit if we already have an ingest record
        Call<Response<Ingest>> get = events.getIngests(ImmutableMap.of("bag", bag.getUuid()));
        retrofit2.Response<Response<Ingest>> execute = get.execute();
        if (execute.isSuccessful() && execute.body().getCount() > 0) {
            return;
        }

        Ingest record = new Ingest()
                .setIngestId(UUID.randomUUID().toString())
                .setBag(bag.getUuid())
                .setCreatedAt(ZonedDateTime.now())
                .setIngested(true)
                .setReplicatingNodes(bag.getReplicatingNodes());

        Call<Ingest> ingest = events.createIngest(record);
        retrofit2.Response<Ingest> response = ingest.execute();
        if (!response.isSuccessful()) {
            throw new IOException("Could not create ingest record with DPN");
        }
    }

}
