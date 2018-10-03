package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableSet;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.enums.BagStatus;
import org.slf4j.Logger;
import retrofit2.Call;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class validates BagReceipts against the Chronopolis Ingest server
 *
 * Created by shake on 6/1/16.
 */
public class ChronopolisCheck extends Checker {

    private final Logger log;
    private final DepositorService depositors;
    private final Bicarbonate cleaningManager;

    public ChronopolisCheck(BagData data,
                            List<BagReceipt> receipts,
                            BridgeContext context,
                            BridgeAPI bridge,
                            DepositorService depositors,
                            Bicarbonate cleaningManager) {
        super(data, receipts, context, bridge);
        this.log = context.getLogger();
        this.depositors = depositors;
        this.cleaningManager = cleaningManager;
    }

    @Override
    protected void checkReceipts(BagReceipt receipt,
                                 BagData data,
                                 AtomicInteger accumulator,
                                 Map<String, ReplicationHistory> history) {
        String snapshot = data.snapshotId();
        log.info("[CCheck] Processing {}", snapshot);

        SimpleCallback<Bag> callback = new SimpleCallback<>();
        Call<Bag> bagCall = depositors.getDepositorBag(data.depositor(), receipt.getName());
        bagCall.enqueue(callback);
        Set<String> replicatingNodes = callback.getResponse()
                .filter(bag -> bag.getStatus() == BagStatus.PRESERVED)
                .filter(bag -> cleaningManager.forChronopolis(bag).apply(log))
                .map(Bag::getReplicatingNodes).orElse(ImmutableSet.of());

        for (String node : replicatingNodes) {
            accumulator.incrementAndGet();
            ReplicationHistory h = history.getOrDefault(node,
                    new ReplicationHistory(snapshot, node, false));
            h.addReceipt(receipt.getName());
            history.put(node, h);
        }
    }
}
