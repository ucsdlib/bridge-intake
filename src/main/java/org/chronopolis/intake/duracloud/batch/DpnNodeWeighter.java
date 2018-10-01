package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.models.Node;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.DPN;
import org.chronopolis.intake.duracloud.model.BagData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

/**
 * Basic supplier to get the weighted values of nodes for
 * a snapshot. This is done by appending the replicating node's name
 * to the end of the snapshot, then ordering them for their weighted
 * values.
 *
 * See: Rendezvous hashing
 *
 * Created by shake on 6/5/17.
 */
public class DpnNodeWeighter implements Supplier<List<Weight>> {
    private final Logger log = LoggerFactory.getLogger(DpnNodeWeighter.class);

    private final BagData data;
    private final BalustradeNode nodeAPI;
    private final IntakeSettings settings;

    public DpnNodeWeighter(BagData data, BalustradeNode nodeAPI, IntakeSettings settings) {
        this.data = data;
        this.nodeAPI = nodeAPI;
        this.settings = settings;
    }

    @Override
    public List<Weight> get() {
        List<String> replicatingNodes = loadNode(settings);
        return replicatingNodes.stream()
                .map(node -> new Weight(node, data.snapshotId()))
                .sorted(comparing(w -> w.getCode().asLong(), reverseOrder()))
                .collect(Collectors.toList());
    }

    private List<String> loadNode(IntakeSettings settings) {
        DPN cfg = settings.getDpn();
        List<String> nodes;
        Response<Node> response = null;
        Call<Node> call = nodeAPI.getNode(cfg.getUsername());
        try {
            response = call.execute();
        } catch (IOException e) {
            log.error("Error communicating with server", e);
        }

        if (response != null && response.isSuccessful()) {
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


}
