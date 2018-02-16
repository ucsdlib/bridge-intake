package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.collect.ImmutableMap;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.earth.SimpleCallback;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import retrofit2.Call;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Create replications in the DPN Registry
 *
 * @author shake
 */
public class DpnReplicate implements BiConsumer<Bag, List<Weight>> {

    private final String depositor;
    private final IntakeSettings settings;
    private final BagStagingProperties staging;
    private final BalustradeTransfers transfers;

    public DpnReplicate(String depositor,
                        IntakeSettings settings,
                        BagStagingProperties staging,
                        BalustradeTransfers transfers) {
        this.depositor = depositor;
        this.settings = settings;
        this.staging = staging;
        this.transfers = transfers;
    }

    /**
     * Create replications for a bag
     *
     * Search for replications already created
     *  -> if none exist, create both
     *  -> if one exists, create the missing
     *
     * @param bag     the bag to replicate
     * @param weights the replicating nodes, weighted
     */
    @Override
    public void accept(Bag bag, List<Weight> weights) {
        final ImmutableMap<String, String> params = ImmutableMap.of("bag", bag.getUuid());

        Call<Response<Replication>> call = transfers.getReplications(params);
        SimpleCallback<Response<Replication>> rcb = new SimpleCallback<>();
        call.enqueue(rcb);

        rcb.getResponse().ifPresent(response -> {
            if (response.getCount() == 0) {
                pushReplication(bag, weights, (weight) -> true);
            } else if (response.getCount() == 1) {
                Replication exists = response.getResults().get(0);
                pushReplication(bag, weights, doesNotExist(exists.getToNode()));
            }
        });
    }

    /**
     * Use the weighted list of nodes to create replications for a given bag. Before creating a
     * replication, use a {@link Predicate} to test if the node in the weight can be used.
     *
     * @param bag       the bag to create replications for
     * @param weights   the possible replicating nodes, weighted
     * @param predicate the predicate to test each weight
     */
    private void pushReplication(Bag bag, List<Weight> weights, Predicate<Weight> predicate) {
        weights.stream()
                .limit(2)
                .filter(predicate)
                .map(weight -> call(bag, weight.getNode()))
                .forEach(call -> call.enqueue(new SimpleCallback<>()));
    }

    /**
     * {@link Predicate} to test that a node does not match an existing value
     *
     * @param existing the existing node name to test against
     * @return if the node attributed to the weight does not equal the existing node name
     */
    private Predicate<Weight> doesNotExist(String existing) {
        return (weight) -> !weight.getNode().equalsIgnoreCase(existing);
    }

    /**
     * Create an http request for creating a replication for a given bag, but do not execute it
     *
     * @param bag the bag to create the replication request for
     * @param to  the node receiving the request
     * @return the http request to execute
     */
    private Call<Replication> call(Bag bag, String to) {
        final String PROTOCOL = "rsync";
        final String ALGORITHM = "sha256";

        Posix posix = staging.getPosix();
        Path save = Paths.get(posix.getPath(), depositor, bag.getUuid() + ".tar");
        String ourNode = settings.getDpn().getUsername();

        Replication replication = new Replication()
                .setCreatedAt(ZonedDateTime.now())
                .setUpdatedAt(ZonedDateTime.now())
                .setReplicationId(UUID.randomUUID().toString())
                .setFromNode(ourNode)
                .setToNode(to)
                .setLink(to + "@" + settings.getDpnReplicationServer() + ":" + save.toString())
                .setProtocol(PROTOCOL)
                .setStored(false)
                .setStoreRequested(false)
                .setCancelled(false)
                .setBag(bag.getUuid())
                .setFixityAlgorithm(ALGORITHM);

        return transfers.createReplication(replication);
    }
}
