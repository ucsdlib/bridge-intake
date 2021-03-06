package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.cleaner.TrueCleaner;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.test.support.CallWrapper;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the ChronopolisCheck
 * <p>
 * Tests todo:
 * - multiple bags returned
 * - cleaning fail
 * <p>
 * Created by shake on 6/1/16.
 */
public class ChronopolisCheckTest extends BatchTestBase {

    // Mocks for our http apis
    @Mock private BridgeAPI bridge = mock(BridgeAPI.class);
    @Mock private Bicarbonate cleaningManager = mock(Bicarbonate.class);
    @Mock private DepositorService depositors = mock(DepositorService.class);

    private BridgeContext context = new BridgeContext(bridge, "pre", "manifest", "restores",
            "snapshots", Push.CHRONOPOLIS, "chronopolis-check-test");
    private ChronopolisCheck check;

    @Test
    public void testCompleteSnapshot() {
        BagData bagData = data();
        BagReceipt receipt = receipt();

        ImmutableList<BagReceipt> receipts = ImmutableList.of(receipt);
        Bag bag = createChronBagFullReplications();

        String depositor = bagData.depositor();
        String receiptName = receipt.getName();
        CallWrapper<Bag> bagGetWrapper = new CallWrapper<>(bag);

        // all our mocks...
        // 1. create the bag service
        // 2. GET /api/depositors/{depositor}/{bag}
        // 3. clean staging
        // 4. POST /snapshot/{id}/history
        // 5. POST /snapshot/{id}/complete
        when(depositors.getDepositorBag(eq(depositor), eq(receiptName)))
                .thenReturn(bagGetWrapper);
        when(cleaningManager.forChronopolis(eq(bag))).thenReturn(new TrueCleaner());
        when(bridge.postHistory(any(String.class), any(History.class)))
                .thenReturn(new CallWrapper<>(new HistorySummary()));
        when(bridge.completeSnapshot(any(String.class), any(AlternateIds.class)))
                .thenReturn(new CallWrapper<>(new SnapshotComplete()));

        check = new ChronopolisCheck(bagData, receipts, context, depositors, cleaningManager);
        check.run();

        verify(depositors, times(1)).getDepositorBag(eq(depositor), eq(receiptName));
        verify(cleaningManager, times(1)).forChronopolis(eq(bag));
        verify(bridge, times(3)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(1)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

    @Test
    public void testIncompleteSnapshot() {
        when(depositors.getDepositorBag(anyString(), anyString()))
                .thenReturn(new CallWrapper<>(createChronBagPartialReplications()));

        check = new ChronopolisCheck(data(), receipts(), context, depositors, cleaningManager);
        check.run();
        verify(depositors, times(2)).getDepositorBag(anyString(), anyString());
        verify(bridge, times(0)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(0)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

}