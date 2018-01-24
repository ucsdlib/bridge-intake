package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageImpl;
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
    private final Logger log = LoggerFactory.getLogger(ChronopolisCheck.class);

    private final BagService bagService;
    private final Bicarbonate cleaningManager;

    public ChronopolisCheck(BagData data,
                            List<BagReceipt> receipts,
                            BridgeAPI bridge,
                            ServiceGenerator generator,
                            Bicarbonate cleaningManager) {
        super(data, receipts, bridge);
        this.bagService = generator.bags();
        this.cleaningManager = cleaningManager;
    }

    @Override
    protected void checkReceipts(BagReceipt receipt,
                                 BagData data,
                                 AtomicInteger accumulator,
                                 Map<String, ReplicationHistory> history) {
        String snapshot = data.snapshotId();
        log.info("[CCheck] Processing {}", snapshot);

        // honestly should just be <String, String>
        ImmutableMap<String, Object> params =
                ImmutableMap.of("depositor", data.depositor(),
                                "name", receipt.getName());
        Call<PageImpl<Bag>> bags = bagService.get(params);
        SimpleCallback<PageImpl<Bag>> cb = new SimpleCallback<>();
        bags.enqueue(cb);
        Set<String> replicatingNodes = cb.getResponse()
                .filter(page -> page.getTotalElements() == 1)   // filter on having one element
                .map(page -> page.getContent().get(0)) // get the head
                .filter(bag -> bag.getStatus() == BagStatus.PRESERVED)
                .filter(bag -> cleaningManager.forChronopolis(bag).call()) // attempt to clean
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
