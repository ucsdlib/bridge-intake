package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Ingest;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.cleaner.TrueCleaner;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for DpnCheck
 *
 * Tests todo:
 * - ingest create fail
 * - cleaning dpn fail
 * - cleaning chron fail
 *
 * Created by shake on 6/1/16.
 */
public class DpnCheckTest extends BatchTestBase {

    // Mocks for our http apis
    @Mock private Events events;
    @Mock private BridgeAPI bridge;
    @Mock private BalustradeTransfers transfers;
    @Mock private BalustradeNode nodes;
    @Mock private BalustradeBag bags;
    @Mock private Bicarbonate cleaningManager;

    // And our test object
    private DpnCheck check;

    // Dependency to the DpnCheck
    private LocalAPI dpn;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        dpn = new LocalAPI();
        dpn.setTransfersAPI(transfers);
        dpn.setNodeAPI(nodes);
        dpn.setBagAPI(bags);
        dpn.setEventsAPI(events);

        check = new DpnCheck(data(), receipts(), bridge, dpn, cleaningManager);
    }

    @Test
    public void testCompleteSnapshot() {
        when(bags.getBag(any(String.class))).thenReturn(new CallWrapper<>(createBagFullReplications()));
        when(events.getIngests(any())).thenReturn(new CallWrapper<>(new Response<>()));
        when(events.createIngest(any(Ingest.class))).thenReturn(new CallWrapper<>(new Ingest()));
        when(cleaningManager.cleaner(any())).thenReturn(new TrueCleaner());
        when(cleaningManager.forChronopolis(anyString(), anyString())).thenReturn(new TrueCleaner());
        when(bridge.postHistory(any(String.class), any(History.class))).thenReturn(new CallWrapper<>(new HistorySummary()));
        when(bridge.completeSnapshot(any(String.class), any(AlternateIds.class))).thenReturn(new CallWrapper<>(new SnapshotComplete()));

        check.run();

        verify(bags, times(2)).getBag(any(String.class));
        verify(bridge, times(3)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(1)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

    @Test
    public void testIncompleteSnapshot() {
        when(bags.getBag(any(String.class))).thenReturn(new CallWrapper<>(createBagPartialReplications()));

        check.run();

        verify(bags, times(2)).getBag(any(String.class));
        verify(bridge, times(0)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(0)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

}