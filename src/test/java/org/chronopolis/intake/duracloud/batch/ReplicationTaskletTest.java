package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Node;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.intake.duracloud.test.TestApplication;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.IngestRequest;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import retrofit2.Call;
import retrofit2.Callback;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests completed:
 * - Creation of DPN Bag
 * - Creation of DPN Replications
 * - Creation of Chronopolis Bag
 * - Updates to the bridge
 * - ReplicationHistory ONLY when all DPN replications are stored (single + multiple receipts)
 * - Closing snapshots ""
 *
 * <p/>
 * Created by shake on 12/4/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TestApplication.class)
public class ReplicationTaskletTest {
    private final Logger log = LoggerFactory.getLogger(ReplicationTaskletTest.class);
    private final String MEMBER = "test-member";
    private final String NAME = "test-name";
    private final String DEPOSITOR = "test-depositor";
    private final String SNAPSHOT_ID = "test-snapshot-id";

    // Objects return by http apis
    Node myDpnNode;

    // Startup beans
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired IntakeSettings settings;

    // Mocks for our http apis
    @Mock BridgeAPI bridge;
    @Mock IngestAPI ingest;
    @Mock LocalAPI dpn;

    // We return these later
    @Mock BalustradeTransfers transfers;
    @Mock BalustradeNode nodes;
    @Mock BalustradeBag bags;

    // Our reader so we don't need real fs access
    @Mock ReplicationTasklet.ReaderFactory factory;
    @Mock DpnInfoReader reader;

    // And our test object
    @InjectMocks ReplicationTasklet tasklet;

    BagData initializeBagData() {
        BagData data = new BagData();
        data.setMember(MEMBER);
        data.setName(NAME);
        data.setDepositor(DEPOSITOR);
        data.setSnapshotId(SNAPSHOT_ID);
        return data;
    }

    Node initializeNode() {
        Node node = new Node();
        node.setName(settings.getNode());
        node.setReplicateTo(ImmutableList.of(UUID.randomUUID().toString(),
                UUID.randomUUID().toString()));
        return node;
    }

    BagReceipt bagReceipt() {
        BagReceipt receipt = new BagReceipt();
        receipt.setName(UUID.randomUUID().toString());
        receipt.setReceipt(UUID.randomUUID().toString());
        return receipt;
    }

    // @Before
    public void before() throws Exception {
        BagData data = initializeBagData();

        List<BagReceipt> receipts = new ArrayList<>();
        receipts.add(bagReceipt());

        myDpnNode = initializeNode();

        tasklet = new ReplicationTasklet(data, receipts, bridge, ingest, dpn, settings);
        MockitoAnnotations.initMocks(this);
    }

    // Helpers for our tests

    List<BagReceipt> initialize(int numReceipts) {
        BagData data = initializeBagData();

        int added = 0;
        List<BagReceipt> receipts = new ArrayList<>();
        while (added < numReceipts) {
            receipts.add(bagReceipt());
            added++;
        }

        myDpnNode = initializeNode();

        tasklet = new ReplicationTasklet(data, receipts, bridge, ingest, dpn, settings);
        MockitoAnnotations.initMocks(this);

        return receipts;
    }

    Replication createReplication(Replication.Status status) {
        Replication r = new Replication();
        r.setFromNode(settings.getNode());
        r.setToNode(UUID.randomUUID().toString());
        r.setStatus(status);
        return r;
    }

    Bag createBagNoReplications(BagReceipt receipt) {
        Bag b = new Bag();
        b.setUuid(receipt.getName());
        b.setLocalId("local-id");
        b.setFirstVersionUuid(b.getUuid());
        b.setIngestNode("test-node");
        b.setAdminNode("test-node");
        b.setBagType('D');
        b.setMember(MEMBER);
        b.setCreatedAt(DateTime.now());
        b.setUpdatedAt(DateTime.now());
        b.setSize(10L);
        b.setVersion(1L);
        b.setInterpretive(new ArrayList<String>());
        b.setReplicatingNodes(new ArrayList<String>());
        b.setRights(new ArrayList<String>());
        b.setFixities(ImmutableMap.of("fixity-algorithm", "fixity-value"));
        return b;
    }

    Bag createBagFullReplications(BagReceipt receipt) {
        Bag b = createBagNoReplications(receipt);
        b.setReplicatingNodes(ImmutableList.of("test-repl-1", "test-repl-2", "test-repl-3"));
        return b;
    }

    Bag createBagPartialReplications(BagReceipt receipt) {
        Bag b = createBagNoReplications(receipt);
        b.setReplicatingNodes(ImmutableList.of("test-repl-1"));
        return b;
    }

    Call<Response<Replication>> createResponse(List<Replication> results) {
        Response<Replication> r = new Response<>();
        r.setResults(results);
        r.setCount(results.size());
        return new CallWrapper<>(r);
    }

    // setting up responses for our mock objects

    void readyBagMocks() throws IOException {
        // dpn reader stuffs
        when(factory.reader(any(Path.class), anyString())).thenReturn(reader);
        when(reader.getLocalId()).thenReturn(SNAPSHOT_ID);
        when(reader.getRightsIds()).thenReturn(ImmutableList.<String>of());
        when(reader.getVersionNumber()).thenReturn(Long.valueOf(1));
        when(reader.getIngestNodeName()).thenReturn(settings.getNode());
        when(reader.getInterpretiveIds()).thenReturn(ImmutableList.<String>of());
        when(reader.getFirstVersionUUID()).thenReturn(UUID.randomUUID().toString());

        // bag api
        when(dpn.getBagAPI()).thenReturn(bags);
        when(ingest.stageBag(any(IngestRequest.class)))
                .thenReturn(new CallWrapper<>(new org.chronopolis.rest.entities.Bag("test", "test")));
    }

    void readyReplicationMocks(String name, Replication.Status r1, Replication.Status r2) {
        when(transfers.getReplications(ImmutableMap.of("uuid", name)))
                .thenReturn(createResponse(ImmutableList.<Replication>of(
                        createReplication(r1),
                        createReplication(r2))));
    }

    //
    // Tests
    //

    /**
     * Test where we check that both replications and bags were created
     * HTTP Calls look like:
     * 1. GET bag -> 404
     * 2. GET replications -> 404
     * 3. POST bag -> 201
     * 4.
     *
     * @throws Exception
     */
    @Test
    public void testCreateBagAndReplications() throws Exception {
        List<BagReceipt> receipts = initialize(1);
        readyBagMocks();
        Bag b = createBagNoReplications(receipts.get(0));

        // result is ignored so just return an empty bag
        // TODO: Be more strict about what we pass in
        when(bags.getBag(any(String.class))).thenReturn(new NotFoundWrapper<Bag>(null));
        when(bags.createBag(any(Bag.class))).thenReturn(new CallWrapper<>(b));

        // set up to return our dpn replications
        when(dpn.getTransfersAPI()).thenReturn(transfers);
        when(transfers.getReplications(anyMap()))
                .thenReturn(createResponse(new ArrayList<Replication>()));

        // result is ignored so just return an empty replication
        when(transfers.createReplication(any(Replication.class)))
                .thenReturn(new CallWrapper<>(new Replication()));

        // set up our returned node
        when(dpn.getNodeAPI()).thenReturn(nodes);
        when(nodes.getNode(anyString())).thenReturn(new CallWrapper<>(myDpnNode));

        // run the tasklet
        tasklet.run();

        // TODO: We can verify against all mocks, not sure if we need that though
        // verify that these were actually called
        verify(reader, times(1)).getLocalId();
        verify(reader, times(1)).getRightsIds();
        verify(reader, times(1)).getVersionNumber();
        verify(reader, times(1)).getIngestNodeName();
        verify(reader, times(1)).getInterpretiveIds();
        verify(reader, times(1)).getFirstVersionUUID();
        // verify(bags, times(1)).createBag(any(Bag.class), any(Callback.class));
        verify(transfers, times(2)).createReplication(any(Replication.class));
        verify(ingest, times(1)).stageBag(any(IngestRequest.class));
    }

    /**
     * Test where we check that all replications have been stored
     *
     * @throws IOException
     */
    @Test
    public void testCheckStoredReplications() throws IOException {
        List<BagReceipt> receipts = initialize(2);
        readyBagMocks();

        // Create bags with full replications
        // And prime our mock
        for (BagReceipt receipt : receipts) {
            Bag bag = createBagFullReplications(receipt);
            when(bags.getBag(bag.getUuid())).thenReturn(new CallWrapper<>(bag));
        }


        when(dpn.getTransfersAPI()).thenReturn(transfers);

        // TODO: Instantiate this bag somewhere else
        // result is ignored so just return an empty bag
        // when(bags.createBag(any(Bag.class))).thenReturn(new CallWrapper<>(new Bag()));

        for (BagReceipt receipt : receipts) {
            readyReplicationMocks(receipt.getName(), Replication.Status.STORED, Replication.Status.STORED);
        }

        // Prepare our history
        when(bridge.postHistory(anyString(), any(History.class))).thenReturn(new CallWrapper<>(new HistorySummary()));
        when(bridge.completeSnapshot(anyString(), any(AlternateIds.class))).thenReturn(new CallWrapper<>(new SnapshotComplete()));

        tasklet.run();

        // verify all our mocks
        // 2 receipts -> 2 getBag calls
        // 3 replicating nodes -> 3 postHistory calls
        // 1 snapshot -> 1 snapshotComplete
        verify(bags, times(2)).getBag(anyString());
        verify(bridge, times(3)).postHistory(anyString(), any(History.class));
        verify(bridge, times(1)).completeSnapshot(anyString(), any(AlternateIds.class));
    }

    /**
     * Test where we check that not all replications have been finished
     *
     * @throws IOException
     */
    @Test
    public void testPartiallyStoredReplications() throws IOException {
        List<BagReceipt> receipts = initialize(2);

        // Create two bags, one fully replicated, one partially
        Bag fullyRepl = createBagFullReplications(receipts.get(0));
        Bag partialRepl = createBagPartialReplications(receipts.get(1));

        readyBagMocks();

        // Prepare our mocks
        when(bags.getBag(fullyRepl.getUuid())).thenReturn(new CallWrapper<>(fullyRepl));
        when(bags.getBag(partialRepl.getUuid())).thenReturn(new CallWrapper<>(partialRepl));
        when(dpn.getTransfersAPI()).thenReturn(transfers);

        // result is ignored so just return an empty bag
        when(bags.createBag(any(Bag.class))).thenReturn(new CallWrapper<>(new Bag()));
        int i = 0;
        for (BagReceipt receipt : receipts) {
            // We want one receipt to be complete, and one incomplete
            if (i == 0) {
                readyReplicationMocks(receipt.getName(), Replication.Status.STORED, Replication.Status.STORED);
            } else {
                readyReplicationMocks(receipt.getName(), Replication.Status.STORED, Replication.Status.CONFIRMED);
            }
            i++;
        }

        tasklet.run();

        // make sure we don't close the snapshot
        verify(bridge, times(0)).postHistory(anyString(), any(History.class));
        verify(bridge, times(0)).completeSnapshot(anyString(), any(AlternateIds.class));
    }


    public class CallWrapper<E> implements Call<E> {

        E e;

        public CallWrapper(E e) {
            this.e = e;
        }

        @Override
        public retrofit2.Response<E> execute() throws IOException {
            return retrofit2.Response.success(e);
        }

        @Override
        public void enqueue(Callback<E> callback) {
            callback.onResponse(retrofit2.Response.success(e));
        }

        @Override
        public boolean isExecuted() {
            return false;
        }

        @Override
        public void cancel() {
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public Call<E> clone() {
            return null;
        }
    }

    public class NotFoundWrapper<E> extends CallWrapper<E> {

        public NotFoundWrapper(E e) {
            super(e);
        }

        @Override
        public retrofit2.Response<E> execute() throws IOException {
            return retrofit2.Response.error(404, ResponseBody.create(MediaType.parse("application/json"), ""));
        }

        @Override
        public void enqueue(Callback<E> callback) {
            callback.onResponse(retrofit2.Response.<E>error(404, ResponseBody.create(MediaType.parse("application/json"), "")));
        }

    }

}