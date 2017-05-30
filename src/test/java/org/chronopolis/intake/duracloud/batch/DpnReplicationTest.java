package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
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
 * TODO: Possibly have a super class which holds the
 * creators for some of our objects (bags, replications, etc)
 *
 * Tests completed:
 * - Creation of DPN Bag
 * - Creation of DPN Replications
 *
 * Tests to do:
 * - No bag present -> no replications created
 * - Bag present, repls -> no additional creation
 * - Bag present, no repls -> create repls
 *
 * Created by shake on 12/4/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class DpnReplicationTest extends BatchTestBase {
    private final Logger log = LoggerFactory.getLogger(DpnReplicationTest.class);

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

    // Helpers for our tests

    // Pretty ugly, we'll want to find a better way to handle init
    private List<BagReceipt> initialize(int numReceipts) {
        List<String> replicatingNodes = ImmutableList.of(UUID.randomUUID().toString(),
                UUID.randomUUID().toString());
        BagData data = data();

        int added = 0;
        List<BagReceipt> receipts = new ArrayList<>();
        while (added < numReceipts) {
            receipts.add(receipt());
            added++;
        }

        dpn = new LocalAPI();
        tasklet = new DpnReplication(data, receipts, replicatingNodes, dpn, settings);
        MockitoAnnotations.initMocks(this);

        dpn.setBagAPI(bags)
                .setTransfersAPI(transfers)
                .setNodeAPI(nodes);

        return receipts;
    }

    private Replication createReplication(boolean stored) {
        Replication r = new Replication();
        r.setFromNode(settings.getChron().getNode());
        r.setToNode(UUID.randomUUID().toString());
        r.setStored(stored);
        return r;
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

    private Bag createBagFullReplications(BagReceipt receipt) {
        Bag b = createBagNoReplications(receipt);
        b.setReplicatingNodes(ImmutableList.of("test-repl-1", "test-repl-2", "test-repl-3"));
        return b;
    }

    private Bag createBagPartialReplications(BagReceipt receipt) {
        Bag b = createBagNoReplications(receipt);
        b.setReplicatingNodes(ImmutableList.of("test-repl-1"));
        return b;
    }

    private Call<Response<Replication>> createResponse(List<Replication> results) {
        Response<Replication> r = new Response<>();
        r.setResults(results);
        r.setCount(results.size());
        return new CallWrapper<>(r);
    }

    // setting up responses for our mock objects

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

    private void readyReplicationMocks(String name, boolean stored) {
        when(transfers.getReplications(ImmutableMap.of("bag", name)))
                .thenReturn(createResponse(ImmutableList.of(
                        createReplication(true),
                        createReplication(stored))));
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
     * @throws Exception Not actually thrown bc we use the ReaderFactory to inject a mock
     */
    @Test
    public void testCreateBagAndReplications() throws Exception {
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

        // TODO: We can verify against all mocks, not sure if we need that though
        //       we probably should though to ensure no calls are made that we don't expect
        // verify that these were actually called
        verify(reader, times(1)).getLocalId();
        verify(reader, times(1)).getRightsIds();
        verify(reader, times(1)).getVersionNumber();
        // verify(reader, times(1)).getIngestNodeName();
        verify(reader, times(1)).getInterpretiveIds();
        verify(reader, times(1)).getFirstVersionUUID();
        verify(transfers, times(2)).createReplication(any(Replication.class));
    }

}