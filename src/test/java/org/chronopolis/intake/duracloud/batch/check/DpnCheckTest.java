package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for DpnCheck
 * TODO: Test IOException
 *
 * Created by shake on 6/1/16.
 */
public class DpnCheckTest extends BatchTestBase {

    // Mocks for our http apis
    @Mock BridgeAPI bridge;
    @Mock BalustradeTransfers transfers;
    @Mock BalustradeNode nodes;
    @Mock BalustradeBag bags;

    // And our test object
    DpnCheck check;

    // Dependency to the DpnCheck
    LocalAPI dpn;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        dpn = new LocalAPI();
        dpn.setTransfersAPI(transfers);
        dpn.setNodeAPI(nodes);
        dpn.setBagAPI(bags);

        check = new DpnCheck(data(), receipts(), bridge, dpn);
    }

    @Test
    public void testCompleteSnapshot() {
        when(bags.getBag(any(String.class))).thenReturn(new CallWrapper<>(createBagFullReplications()));
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