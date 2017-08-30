package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.batch.support.BadRequestWrapper;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.batch.support.ExceptingWrapper;
import org.chronopolis.intake.duracloud.batch.support.NotFoundWrapper;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringRunner;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TODO: Possibly have a super class which holds the
 * creators for some of our objects (bags, replications, etc)
 *
 * Tests completed:
 * - Exception getting bag -> no creation
 * - Bag present, no repls -> create repls
 * - Bag and repls present -> no actions
 * - No Bag, -> create bag + replications
 * - Bag create fail -> no creation of replications
 * - Reader fail -> no creation
 * - Bag present, single repl -> create additional
 *
 * Created by shake on 12/4/15.
 */
@RunWith(SpringRunner.class)
public class DpnReplicationTest extends BatchTestBase {

    // We return these later
    @Mock private BalustradeTransfers transfers;
    @Mock private BalustradeNode nodes;
    @Mock private BalustradeBag bags;

    // Our reader so we don't need real fs access
    @Mock private DpnReplication.ReaderFactory factory;
    @Mock private DpnInfoReader reader;

    // And our test object
    @InjectMocks
    private DpnReplication tasklet;

    private LocalAPI dpn;
    private List<Weight> weights;

    // Helpers for our tests

    // Pretty ugly, we'll want to find a better way to handle init
    private List<BagReceipt> initialize(int numReceipts) {
        weights = ImmutableList.of(new Weight(UUID.randomUUID().toString(), "snapshot"),
                new Weight(UUID.randomUUID().toString(), "snapshot"),
                new Weight(UUID.randomUUID().toString(), "snapshot"));
        BagData data = data();

        List<BagReceipt> receipts = IntStream.range(0, numReceipts)
                .mapToObj(i -> receipt())
                .collect(Collectors.toList());

        dpn = new LocalAPI();
        tasklet = new DpnReplication(data, receipts, weights, dpn, settings, stagingProperties);
        MockitoAnnotations.initMocks(this);

        dpn.setBagAPI(bags)
                .setTransfersAPI(transfers)
                .setNodeAPI(nodes);

        return receipts;
    }

    ///
    // Tests
    ///

    /**
     * Test an exception communicating when trying to get the bag. No operations
     * should be done after.
     */
    @Test
    public void bagException() {
        initialize(1);
        when(bags.getBag(anyString())).thenReturn(new ExceptingWrapper<>());

        tasklet.run();
        verify(bags, times(1)).getBag(anyString());
        verify(bags, times(0)).createBag(any(Bag.class));
        verify(bags, times(0)).createDigest(anyString(), any(Digest.class));
        verify(transfers, times(0)).getReplications(anyMap());
        verify(transfers, times(0)).createReplication(any(Replication.class));
    }

    /**
     * Test to create replications for an existing bag
     */
    @Test
    public void bagExistsCreateReplications() {
        initialize(2);
        Bag b = createBagNoReplications(receipt());

        when(bags.getBag(anyString())).thenReturn(new CallWrapper<>(b));
        when(transfers.getReplications(anyMap()))
                .thenReturn(createResponse(new ArrayList<>()));
        when(transfers.createReplication(any(Replication.class)))
                .thenReturn(new CallWrapper<>(new Replication()));

        tasklet.run();

        verify(bags, times(2)).getBag(anyString());
        verify(bags, times(0)).createBag(any(Bag.class));
        verify(transfers, times(2)).getReplications(anyMap());
        verify(transfers, times(4)).createReplication(any(Replication.class));
    }

    /**
     * Test that no action is taken when a bag and replications exist
     */
    @Test
    public void bagAndReplicationsExist() {
        initialize(1);
        Bag b = createBagNoReplications(receipt());

        // Our replications
        Weight one = weights.get(0);
        Weight two = weights.get(1);
        ImmutableList<Replication> replications =
                ImmutableList.of(createReplication(one.getNode()), createReplication(two.getNode()));

        // Mock setups
        when(bags.getBag(anyString())).thenReturn(new CallWrapper<>(b));
        when(transfers.getReplications(anyMap()))
                .thenReturn(createResponse(replications));

        tasklet.run();

        // validations
        verify(bags, times(1)).getBag(anyString());
        verify(bags, times(0)).createBag(any(Bag.class));
        verify(transfers, times(1)).getReplications(anyMap());
        verify(transfers, times(0)).createReplication(any(Replication.class));
    }

    /**
     * Test where we check that both replications and bags were created
     * HTTP Calls look like:
     * 1. GET bag -> 404
     * 2. POST bag -> 201
     * 3. GET replications -> 200, no content
     * 4. POST replication -> 201 (x2)
     *
     * @throws Exception Not actually thrown bc we use the ReaderFactory to inject a mock
     */
    @Test
    public void createBagAndReplications() throws Exception {
        List<BagReceipt> receipts = initialize(1);
        readyBagMocks();
        Bag b = createBagNoReplications(receipts.get(0));
        Digest d = createDigest(receipts.get(0));

        // result is ignored so just return an empty bag
        // TODO: Be more strict about what we pass in
        when(bags.getBag(any(String.class))).thenReturn(new NotFoundWrapper<>(null));
        when(bags.createBag(any(Bag.class))).thenReturn(new CallWrapper<>(b));
        when(bags.createDigest(eq(b.getUuid()), any(Digest.class))).thenReturn(new CallWrapper<>(d));

        // set up to return our dpn replications
        when(transfers.getReplications(anyMap()))
                .thenReturn(createResponse(new ArrayList<>()));

        // result is ignored so just return an empty replication
        when(transfers.createReplication(any(Replication.class)))
                .thenReturn(new CallWrapper<>(new Replication()));

        // run the tasklet
        tasklet.run();

        // verify that these were actually called
        verify(reader, times(1)).getLocalId();
        verify(reader, times(1)).getRightsIds();
        verify(reader, times(1)).getVersionNumber();
        // verify(reader, times(1)).getIngestNodeName();
        verify(reader, times(1)).getInterpretiveIds();
        verify(reader, times(1)).getFirstVersionUUID();
        verify(transfers, times(2)).createReplication(any(Replication.class));
    }

    /**
     * Test a failure in registering the Bag and Digest; Ensure no replication creation after
     * @throws IOException from readyBagMocks - static method can throw it
     */
    @Test
    public void bagCreateFail() throws IOException {
        List<BagReceipt> receipts = initialize(1);
        readyBagMocks();
        Bag b = createBagNoReplications(receipts.get(0));
        Digest d = createDigest(receipts.get(0));

        when(bags.getBag(any(String.class))).thenReturn(new NotFoundWrapper<>(null));
        when(bags.createBag(any(Bag.class))).thenReturn(new CallWrapper<>(b));
        when(bags.createDigest(eq(b.getUuid()), any(Digest.class))).thenReturn(new BadRequestWrapper<>(d));

        // run the tasklet
        tasklet.run();

        // verify that these were actually called
        verify(reader, times(1)).getLocalId();
        verify(reader, times(1)).getRightsIds();
        verify(reader, times(1)).getVersionNumber();
        // verify(reader, times(1)).getIngestNodeName();
        verify(reader, times(1)).getInterpretiveIds();
        verify(reader, times(1)).getFirstVersionUUID();
        verify(transfers, times(0)).getReplications(anyMap());
        verify(transfers, times(0)).createReplication(any(Replication.class));
    }

    /**
     * Test a failure in the DpnInfo reader. No bag/creation should be done after.
     */
    @Test
    public void readerException() throws IOException {
        initialize(1);
        when(bags.getBag(any(String.class))).thenReturn(new NotFoundWrapper<>(null));
        when(factory.reader(any(Path.class), anyString())).thenThrow(new IOException("test-reader-exception"));

        tasklet.run();

        verify(bags, times(1)).getBag(anyString());
        verify(factory, times(1)).reader(any(Path.class), anyString());
        verify(reader, times(0)).getLocalId();
        verify(reader, times(0)).getRightsIds();
        verify(reader, times(0)).getVersionNumber();
        verify(reader, times(0)).getInterpretiveIds();
        verify(reader, times(0)).getFirstVersionUUID();
        verify(bags, times(0)).createBag(any(Bag.class));
        verify(transfers, times(0)).getReplications(anyMap());
        verify(transfers, times(0)).createReplication(any(Replication.class));
    }

    /**
     * Test that a single replication is created when a previous run failed to
     * create both
     */
    @Test
    public void singleReplicationCreate() {
        initialize(1);
        Bag b = createBagNoReplications(receipt());
        Weight weight = weights.get(0);

        when(bags.getBag(anyString())).thenReturn(new CallWrapper<>(b));
        when(transfers.getReplications(anyMap()))
                .thenReturn(createResponse(ImmutableList.of(createReplication(weight.getNode()))));
        when(transfers.createReplication(any(Replication.class)))
                .thenReturn(new CallWrapper<>(new Replication()));

        tasklet.run();

        verify(bags, times(1)).getBag(anyString());
        verify(bags, times(0)).createBag(any(Bag.class));
        verify(transfers, times(1)).getReplications(anyMap());
        verify(transfers, times(1)).createReplication(any(Replication.class));
    }


    ///
    // Helpers
    ///

    private Replication createReplication(String to) {
        // Fill out the rest of the replication?
        Replication r = new Replication();
        r.setFromNode(settings.getChron().getNode());
        r.setToNode(to);
        return r;
    }

    private void readyBagMocks() throws IOException {
        // dpn reader stuffs
        when(factory.reader(any(Path.class), anyString())).thenReturn(reader);
        when(reader.getLocalId()).thenReturn(SNAPSHOT_ID);
        when(reader.getRightsIds()).thenReturn(ImmutableList.of());
        when(reader.getVersionNumber()).thenReturn(Long.valueOf(1));
        // when(reader.getIngestNodeName()).thenReturn(settings.getChron().getNode());
        when(reader.getInterpretiveIds()).thenReturn(ImmutableList.of());
        when(reader.getFirstVersionUUID()).thenReturn(UUID.randomUUID().toString());
    }

    private Digest createDigest(BagReceipt receipt) {
        Digest d = new Digest();
        d.setNode("test-node");
        d.setAlgorithm("fixity-algorithm");
        d.setValue("fixity-value");
        d.setBag(receipt.getName());
        d.setCreatedAt(ZonedDateTime.now());
        return d;
    }

    private Bag createBagNoReplications(BagReceipt receipt) {
        Bag b = new Bag();
        b.setUuid(receipt.getName());
        b.setLocalId("local-id");
        b.setFirstVersionUuid(b.getUuid());
        b.setIngestNode("test-node");
        b.setAdminNode("test-node");
        b.setBagType('D');
        b.setMember(MEMBER);
        b.setCreatedAt(ZonedDateTime.now());
        b.setUpdatedAt(ZonedDateTime.now());
        b.setSize(10L);
        b.setVersion(1L);
        b.setInterpretive(new ArrayList<>());
        b.setReplicatingNodes(new ArrayList<>());
        b.setRights(new ArrayList<>());
        return b;
    }

    private Call<Response<Replication>> createResponse(List<Replication> results) {
        Response<Replication> r = new Response<>();
        r.setResults(results);
        r.setCount(results.size());
        return new CallWrapper<>(r);
    }

}