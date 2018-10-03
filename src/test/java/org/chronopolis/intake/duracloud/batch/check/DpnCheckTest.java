package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Ingest;
import org.chronopolis.earth.models.Response;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.cleaner.TrueCleaner;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.test.support.CallWrapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    @Mock private Events events = mock(Events.class);
    @Mock private BridgeAPI bridge = mock(BridgeAPI.class);
    @Mock private BalustradeBag bags = mock(BalustradeBag.class);
    @Mock private DepositorService depositors = mock(DepositorService.class);
    @Mock private Bicarbonate cleaningManager = mock(Bicarbonate.class);

    private BridgeContext context = new BridgeContext(bridge, "pre", "manifest", "restores",
            "snapshots", Push.DPN, "dpn-check-test");

    // And our test object
    private DpnCheck check;

    @Before
    public void setup() {
        settings = new IntakeSettings();
        check = new DpnCheck(data(), receipts(), context, bridge, bags, events, depositors,
                cleaningManager, settings);
    }

    @Test
    public void testCompleteSnapshot() {
        // Update the user so that we check appropriately
        Bag bag = createBagFullReplications();
        String oldUser = settings.getDpn().getUsername();
        settings.getDpn().setUsername(bag.getReplicatingNodes().get(0));

        TrueCleaner cleaner = new TrueCleaner();
        CallWrapper<Bag> bagWithReplications = new CallWrapper<>(bag);
        CallWrapper<Response<Ingest>> emptyIngestResponse = new CallWrapper<>(new Response<>());
        CallWrapper<Ingest> ingestResponse = new CallWrapper<>(new Ingest());
        CallWrapper<HistorySummary> historyResponse = new CallWrapper<>(new HistorySummary());
        CallWrapper<SnapshotComplete> completeResponse = new CallWrapper<>(new SnapshotComplete());

        when(bags.getBag(any(String.class))).thenReturn(bagWithReplications);
        when(events.getIngests(any())).thenReturn(emptyIngestResponse);
        when(events.createIngest(any(Ingest.class))).thenReturn(ingestResponse);
        when(cleaningManager.cleaner(any())).thenReturn(cleaner);
        when(cleaningManager.forChronopolis(anyString(), anyString())).thenReturn(cleaner);
        when(bridge.postHistory(any(String.class), any(History.class))).thenReturn(historyResponse);
        when(bridge.completeSnapshot(any(String.class), any(AlternateIds.class)))
                .thenReturn(completeResponse);

        check.run();

        verify(bags, times(2)).getBag(any(String.class));
        verify(bridge, times(3)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(1)).completeSnapshot(any(String.class), any(AlternateIds.class));

        settings.getDpn().setUsername(oldUser);
    }

    @Test
    public void testIncompleteSnapshot() {
        TrueCleaner cleaner = new TrueCleaner();
        Bag dpnBag = createBagPartialReplications();
        when(bags.getBag(any(String.class))).thenReturn(new CallWrapper<>(dpnBag));
        when(depositors.getDepositorBag(anyString(), anyString()))
                .thenReturn(new CallWrapper<>(createChronBagFullReplications()));
        when(bags.updateBag(eq(dpnBag.getUuid()), any())).thenReturn(new CallWrapper<>(dpnBag));
        when(cleaningManager.forChronopolis(anyString(), anyString())).thenReturn(cleaner);

        check.run();

        verify(depositors, times(1)).getDepositorBag(anyString(), anyString());
        verify(bags, times(1)).updateBag(eq(dpnBag.getUuid()), any());
        verify(bags, times(2)).getBag(any(String.class));
        verify(cleaningManager, times(1)).forChronopolis(anyString(), anyString());
        verify(cleaningManager, never()).cleaner(any());
        verify(bridge, times(0)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(0)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

}