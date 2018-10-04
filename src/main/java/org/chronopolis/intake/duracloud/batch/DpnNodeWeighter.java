package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.models.Node;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Constraints;
import org.chronopolis.intake.duracloud.config.props.DPN;
import org.chronopolis.intake.duracloud.constraint.MemberToNodePredicate;
import org.chronopolis.intake.duracloud.constraint.SnapshotSizePredicate;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

/**
 * Basic supplier to get the weighted values of nodes for
 * a snapshot. This is done by appending the replicating node's name
 * to the end of the snapshot, then ordering them for their weighted
 * values.
 * <p>
 * See: Rendezvous hashing
 * <p>
 * Created by shake on 6/5/17.
 */
public class DpnNodeWeighter implements Supplier<List<Weight>> {
    private final Logger log = LoggerFactory.getLogger(DpnNodeWeighter.class);

    private final BalustradeNode nodeAPI;
    private final IntakeSettings settings;
    private final SnapshotDetails details;

    private Map<String, List<String>> memberConstraints;
    private Map<String, Constraints.SizeLimit> bagSizeConstraints;

    public DpnNodeWeighter(BalustradeNode nodeAPI,
                           IntakeSettings settings,
                           SnapshotDetails details) {
        this.nodeAPI = nodeAPI;
        this.settings = settings;
        this.details = details;
        buildConstraintMaps();
    }

    @Override
    public List<Weight> get() {
        List<Predicate<String>> constraints = new ArrayList<>();
        constraints.add(new MemberToNodePredicate(details.getMemberId(), memberConstraints));
        constraints.add(new SnapshotSizePredicate(
                Doubles.tryParse(details.getTotalSizeInBytes()),
                bagSizeConstraints));
        Predicate<String> combinedPredicate = constraints.stream()
                .reduce(Predicate::and)
                .orElse(entry -> true);

        List<String> replicatingNodes = loadNode(settings);
        return replicatingNodes.stream()
                .filter(combinedPredicate)
                .map(node -> new Weight(node, details.getSnapshotId()))
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

    private void buildConstraintMaps() {
        ImmutableMap.Builder<String, List<String>> memberBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, Constraints.SizeLimit> sizeBuilder = ImmutableMap.builder();
        for (Constraints.Node node : settings.getConstraints().getNodes()) {
            sizeBuilder.put(node.getName(), node.getSizeLimit());
            memberBuilder.put(node.getName(), node.getMembers());
        }

        bagSizeConstraints = sizeBuilder.build();
        memberConstraints = memberBuilder.build();
    }

}
