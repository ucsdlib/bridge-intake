package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class validates BagReceipts against the DPN registry
 *
 * Created by shake on 6/1/16.
 */
public class DpnCheck extends Checker {
    private final Logger log = LoggerFactory.getLogger(DpnCheck.class);

    private BalustradeBag bags;

    public DpnCheck(BagData data, List<BagReceipt> receipts, BridgeAPI bridge, LocalAPI dpn) {
        super(data, receipts, bridge);
        this.bags = dpn.getBagAPI();
    }

    @Override
    protected void checkReceipts(BagReceipt receipt, BagData data, AtomicInteger accumulator, Map<String, ReplicationHistory> history) {
        String snapshot = data.snapshotId();
        Call<Bag> call = bags.getBag(receipt.getName());
        try {
            Response<Bag> response = call.execute();
            if (response.isSuccessful()) {
                Bag bag = response.body();

                // Once again, might revisit this
                bag.getReplicatingNodes().forEach(n -> {
                    accumulator.incrementAndGet();
                    ReplicationHistory h = history.getOrDefault(n, new ReplicationHistory(snapshot, n, false));
                    h.addReceipt(bag.getUuid());
                    history.put(n, h);
                });
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

}
