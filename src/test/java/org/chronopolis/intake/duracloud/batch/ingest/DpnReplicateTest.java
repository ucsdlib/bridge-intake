package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.chronopolis.test.support.ExceptingCallWrapper;
import org.junit.Before;
import org.junit.Test;
import retrofit2.Call;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DpnReplicate}
 *
 * @author shake
 */
public class DpnReplicateTest extends BatchTestBase {

    private BalustradeTransfers transfers;

    private Bag bag;
    private Weight weight0;
    private Weight weight1;
    private Weight weight2;
    private List<Weight> weights;
    private DpnReplicate replicate;
    private BridgeContext context;

    @Before
    public void setup() {
        transfers = mock(BalustradeTransfers.class);

        String DEPOSITOR = "test-depositor";
        bag = createBagNoReplications();
        weight0 = new Weight(UUID.randomUUID().toString(), "snapshot");
        weight1 = new Weight(UUID.randomUUID().toString(), "snapshot");
        weight2 = new Weight(UUID.randomUUID().toString(), "snapshot");
        weights = ImmutableList.of(weight0, weight1, weight2);

        context = new BridgeContext(mock(BridgeAPI.class), "prefix", "manifest",
                "restores", "snapshots", Push.DPN, "short-name");

        replicate = new DpnReplicate(DEPOSITOR, context, settings, stagingProperties, transfers);
    }

    @Test
    public void createNone() {
        when(transfers.getReplications(eq(ImmutableMap.of("bag", bag.getUuid()))))
                .thenReturn(response(ImmutableList.of(
                        replication(weight0.getNode()),
                        replication(weight0.getNode()))));

        replicate.accept(bag, weights);
        verify(transfers, times(1)).getReplications(ImmutableMap.of("bag", bag.getUuid()));
        verify(transfers, never()).createReplication(any());
    }

    @Test
    public void createAllSuccess() {
        when(transfers.getReplications(eq(ImmutableMap.of("bag", bag.getUuid()))))
                .thenReturn(response(ImmutableList.of()));
        when(transfers.createReplication(any()))
                .thenReturn(new CallWrapper<>(replication(weight0.getNode())),
                        new CallWrapper<>(replication(weight1.getNode())));

        replicate.accept(bag, weights);
        verify(transfers, times(1)).getReplications(ImmutableMap.of("bag", bag.getUuid()));
        verify(transfers, times(2)).createReplication(any());
    }

    @Test
    public void createOneSuccess() {
        when(transfers.getReplications(eq(ImmutableMap.of("bag", bag.getUuid()))))
                .thenReturn(response(ImmutableList.of(replication(weight2.getNode()))));
        when(transfers.createReplication(any()))
                .thenReturn(new CallWrapper<>(replication(weight0.getNode())));

        replicate.accept(bag, weights);
        verify(transfers, times(1)).getReplications(ImmutableMap.of("bag", bag.getUuid()));
        verify(transfers, times(1)).createReplication(any());
    }

    @Test
    public void createFail() {
        when(transfers.getReplications(eq(ImmutableMap.of("bag", bag.getUuid()))))
                .thenReturn(response(ImmutableList.of()));
        when(transfers.createReplication(any()))
                .thenReturn(
                        new ExceptingCallWrapper<>(replication(weight0.getNode())),
                        new ErrorCallWrapper<>(replication(weight2.getNode()), 404, "not-found"));

        replicate.accept(bag, weights);
        verify(transfers, times(1)).getReplications(ImmutableMap.of("bag", bag.getUuid()));
        verify(transfers, times(2)).createReplication(any());
    }

    private <T> Call<Response<T>> response(List<T> results) {
        Response<T> response = new Response<>();
        response.setResults(results);
        response.setCount(results.size());
        return new CallWrapper<>(response);
    }

    private Replication replication(String to) {
        return new Replication()
                .setToNode(to)
                .setFromNode(settings.getDpn().getUsername());
    }


}