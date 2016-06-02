package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableMap;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.entities.Bag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageImpl;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class validates BagReceipts against the Chronopolis Ingest server
 *
 * Created by shake on 6/1/16.
 */
public class ChronopolisCheck extends Checker {
    private final Logger log = LoggerFactory.getLogger(ChronopolisCheck.class);

    private IngestAPI ingest;

    public ChronopolisCheck(BagData data, List<BagReceipt> receipts, BridgeAPI bridge, IngestAPI ingest) {
        super(data, receipts, bridge);
        this.ingest = ingest;
    }

    @Override
    protected void checkReceipts(BagReceipt receipt, BagData data, AtomicInteger accumulator, Map<String, ReplicationHistory> history) {
        String snapshot = data.snapshotId();

        // honestly should just be <String, String>
        ImmutableMap<String, Object> params =
                ImmutableMap.of("depositor", data.depositor(),
                                "name", data.name());
        Call<PageImpl<Bag>> bags = ingest.getBags(params);
        try {
            Response<PageImpl<Bag>> execute = bags.execute();

            // hmmmm
            // ideally we wouldn't have an iterable bags we're looping over
            // but I suppose this is good enough for a first pass
            execute.body().getContent()
                    .stream()
                    .forEach(b -> {
                        b.getReplicatingNodes().forEach(n -> {
                            accumulator.incrementAndGet();
                            ReplicationHistory h = history.getOrDefault(b, new ReplicationHistory(snapshot, n, false));
                            h.addReceipt(b.getName());
                            history.put(n, h);
                        });
                    });
        } catch (IOException e) {
            log.error("", e);
        }
    }
}
