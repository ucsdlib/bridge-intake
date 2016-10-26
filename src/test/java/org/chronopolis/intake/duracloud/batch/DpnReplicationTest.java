package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
import org.chronopolis.earth.models.Node;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import retrofit2.Call;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TODO: Possibly have a upser class which holds the
 * creators for some of our objects (bags, replications, etc)
 *
 * Tests completed:
 * - Creation of DPN Bag
 * - Creation of DPN Replications
 *
 * <p/>
 * Created by shake on 12/4/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class DpnReplicationTest extends BatchTestBase {
    private final Logger log = LoggerFactory.getLogger(DpnReplicationTest.class);

    Node myDpnNode;

    // We return these later
    @Mock BalustradeTransfers transfers;
    @Mock BalustradeNode nodes;
    @Mock BalustradeBag bags;

    // Our reader so we don't need real fs access
    @Mock DpnReplication.ReaderFactory factory;
    @Mock DpnInfoReader reader;

    // And our test object
    @InjectMocks
    DpnReplication tasklet;
    LocalAPI dpn;

    Node initializeNode() {
        Node node = new Node();
        node.setName(settings.getNode());
        node.setReplicateTo(ImmutableList.of(UUID.randomUUID().toString(),
                UUID.randomUUID().toString()));
        return node;
    }

    // Helpers for our tests

    // Pretty ugly, we'll want to find a better way to handle init
    List<BagReceipt> initialize(int numReceipts) {
        BagData data = data();

        int added = 0;
        List<BagReceipt> receipts = new ArrayList<>();
        while (added < numReceipts) {
            receipts.add(receipt());
            added++;
        }

        myDpnNode = initializeNode();
        dpn = new LocalAPI();
        tasklet = new DpnReplication(data, receipts, dpn, settings);
        MockitoAnnotations.initMocks(this);

        dpn.setBagAPI(bags)
                .setTransfersAPI(transfers)
                .setNodeAPI(nodes);

        return receipts;
    }

    private Replication createReplication(boolean stored) {
        Replication r = new Replication();
        r.setFromNode(settings.getNode());
        r.setToNode(UUID.randomUUID().toString());
        r.setStored(stored);
        return r;
    }

    Digest createDigest(BagReceipt receipt) {
        Digest d = new Digest();
        d.setNode("test-node");
        d.setAlgorithm("fixity-algorithm");
        d.setValue("fixity-value");
        d.setBag(receipt.getName());
        d.setCreatedAt(ZonedDateTime.now());
        return d;
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
        b.setCreatedAt(ZonedDateTime.now());
        b.setUpdatedAt(ZonedDateTime.now());
        b.setSize(10L);
        b.setVersion(1L);
        b.setInterpretive(new ArrayList<>());
        b.setReplicatingNodes(new ArrayList<>());
        b.setRights(new ArrayList<>());
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
        when(reader.getRightsIds()).thenReturn(ImmutableList.of());
        when(reader.getVersionNumber()).thenReturn(Long.valueOf(1));
        when(reader.getIngestNodeName()).thenReturn(settings.getNode());
        when(reader.getInterpretiveIds()).thenReturn(ImmutableList.of());
        when(reader.getFirstVersionUUID()).thenReturn(UUID.randomUUID().toString());
    }

    void readyReplicationMocks(String name, boolean stored1, boolean stored2) {
        when(transfers.getReplications(ImmutableMap.of("bag", name)))
                .thenReturn(createResponse(ImmutableList.of(
                        createReplication(stored1),
                        createReplication(stored2))));

    }


    void readyNodeMock() {
        // set up our returned node
        when(nodes.getNode(anyString())).thenReturn(new CallWrapper<>(myDpnNode));
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
        readyNodeMock();
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

        // TODO: We can verify against all mocks, not sure if we need that though
        // verify that these were actually called
        verify(reader, times(1)).getLocalId();
        verify(reader, times(1)).getRightsIds();
        verify(reader, times(1)).getVersionNumber();
        verify(reader, times(1)).getIngestNodeName();
        verify(reader, times(1)).getInterpretiveIds();
        verify(reader, times(1)).getFirstVersionUUID();
        verify(transfers, times(2)).createReplication(any(Replication.class));
    }

    /**
     * Test where we check that all replications have been stored
     *
     * @throws IOException
     */
    @Test
    public void testCheckStoredReplications() throws IOException {
        List<BagReceipt> receipts = initialize(2);

        readyNodeMock();
        readyBagMocks();

        System.out.println("----------------");
        System.out.println(dpn);
        System.out.println(dpn.getNodeAPI());
        System.out.println("----------------");
        log.info("hello {}", dpn.getNodeAPI());
        log.info("hello {}", dpn.getTransfersAPI());
        log.info("no more hello {}", dpn.getBagAPI());

        // Create bags with full replications
        // And prime our mock
        for (BagReceipt receipt : receipts) {
            Bag bag = createBagFullReplications(receipt);
            when(bags.getBag(bag.getUuid())).thenReturn(new CallWrapper<>(bag));
        }

        for (BagReceipt receipt : receipts) {
            // readyReplicationMocks(receipt.getName(), Replication.Status.STORED, Replication.Status.STORED);
            readyReplicationMocks(receipt.getName(), true, true);
        }

        tasklet.run();

        // verify all our mocks
        // 2 receipts -> 2 getBag calls
        verify(bags, times(2)).getBag(anyString());
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

        readyNodeMock();
        readyBagMocks();

        // Prepare our mocks
        when(bags.getBag(fullyRepl.getUuid())).thenReturn(new CallWrapper<>(fullyRepl));
        when(bags.getBag(partialRepl.getUuid())).thenReturn(new CallWrapper<>(partialRepl));

        // result is ignored so just return an empty bag
        when(bags.createBag(any(Bag.class))).thenReturn(new CallWrapper<>(new Bag()));
        int i = 0;
        for (BagReceipt receipt : receipts) {
            // We want one receipt to be complete, and one incomplete
            if (i == 0) {
                readyReplicationMocks(receipt.getName(), true, true);
            } else {
                readyReplicationMocks(receipt.getName(), true, false);
            }
            i++;
        }

        tasklet.run();

        // TODO: Find mocks to verify
    }

}