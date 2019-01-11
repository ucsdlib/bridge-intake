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
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.constraint.ReplicationDoesNotExist;
import org.slf4j.Logger;
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
@Deprecated
public class DpnReplicate implements BiConsumer<Bag, List<Weight>> {

    private final Logger log;

    private final String depositor;
    private final IntakeSettings settings;
    private final BagStagingProperties staging;
    private final BalustradeTransfers transfers;

    public DpnReplicate(String depositor,
                        BridgeContext context,
                        IntakeSettings settings,
                        BagStagingProperties staging,
                        BalustradeTransfers transfers) {
        this.staging = staging;
        this.settings = settings;
        this.depositor = depositor;
        this.transfers = transfers;
        this.log = context.getLogger();
    }

    /**
     * Create replications for a bag
     * <p>
     * Search for replications already created
     * -> if none exist, create both
     * -> if one exists, create the missing
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
                pushReplication(bag, weights, 2, (weight) -> true);
            } else if (response.getCount() == 1) {
                Replication exists = response.getResults().get(0);
                pushReplication(bag, weights, 1, new ReplicationDoesNotExist(exists.getToNode()));
            }
        });
    }

    /**
     * Use the weighted list of nodes to create replications for a given bag. Before creating a
     * replication, use a {@link Predicate} to test if the node in the weight can be used.
     * <p>
     * The filter (predicate) is run on the list of weights prior to invoking the limit so that we
     * have all options for replications available. e.g. if limit == 1 and our predicate matches the
     * first weight, we still want the next available weight to have a replication created for it.
     *
     * @param bag       the bag to create replications for
     * @param weights   the possible replicating nodes, weighted
     * @param limit     the number of replications to create
     * @param predicate the predicate to test each weight
     */
    private void pushReplication(Bag bag,
                                 List<Weight> weights,
                                 int limit,
                                 Predicate<Weight> predicate) {
        weights.stream()
                .filter(predicate)
                .limit(limit)
                .map(weight -> call(bag, weight.getNode()))
                .forEach(call -> call.enqueue(new SimpleCallback<>()));
    }

    /**
     * Create an http request for creating a replication for a given bag, but do not execute it
     *
     * @param bag the bag to create the replication request for
     * @param to  the node receiving the request
     * @return the http request to execute
     */
    private Call<Replication> call(Bag bag, String to) {
        log.info("[{}] Creating replication for {}", bag.getUuid(), to);
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
