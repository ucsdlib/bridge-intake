package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for checking if a list of BagReceipts has been
 * totally preserved. Calls the abstract method, then updates
 * the bridge with the results
 *
 * Created by shake on 6/1/16.
 */
public abstract class Checker implements Runnable {
    private final Logger log = LoggerFactory.getLogger(Checker.class);

    private BagData data;
    private List<BagReceipt> receipts;
    private BridgeAPI bridge;

    public Checker(BagData data, List<BagReceipt> receipts, BridgeAPI bridge) {
        this.data = data;
        this.receipts = receipts;
        this.bridge = bridge;
    }

    @Override
    public void run() {
        String snapshot = data.snapshotId();
        Map<String, ReplicationHistory> history = new HashMap<>();
        AlternateIds alternates = new AlternateIds();
        AtomicInteger accumulator = new AtomicInteger(0);


        // Might revisit this but for now it seems ok
        receipts.forEach(r -> {
            alternates.addAlternateId(r.getName());
            checkReceipts(r, data, accumulator, history);
        });

        if (accumulator.get() == receipts.size() * 3) {
            for (ReplicationHistory val: history.values()) {
                Call<HistorySummary> call = bridge.postHistory(snapshot, val);
                call.enqueue(new SimpleCallback<>());
            }

            Call<SnapshotComplete> call = bridge.completeSnapshot(snapshot, alternates);
            call.enqueue(new SimpleCallback<>());
        }
    }

    protected abstract void checkReceipts(BagReceipt receipt, BagData data, AtomicInteger accumulator, Map<String, ReplicationHistory> history);

}
