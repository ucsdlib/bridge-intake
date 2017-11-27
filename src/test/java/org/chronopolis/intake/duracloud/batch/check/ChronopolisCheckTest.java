package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.ServiceGenerator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageImpl;

import java.util.Map;

import static org.chronopolis.rest.support.BagConverter.toBagModel;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the ChronopolisCheck
 * TODO: Test IOException
 *
 * Created by shake on 6/1/16.
 */
public class ChronopolisCheckTest extends BatchTestBase {

    // Mocks for our http apis
    @Mock private BridgeAPI bridge;
    @Mock private BagService bagService;
    @Mock private ServiceGenerator generator;
    @Mock private Bicarbonate cleaningManager;

    private ChronopolisCheck check;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCompleteSnapshot() {
        when(generator.bags()).thenReturn(bagService);
        when(bagService.get(any(Map.class))).thenReturn(new CallWrapper<>(new PageImpl<>
                (ImmutableList.of(toBagModel(createChronBagFullReplications())))));
        when(bridge.postHistory(any(String.class), any(History.class))).thenReturn(new CallWrapper<>(new HistorySummary()));
        when(bridge.completeSnapshot(any(String.class), any(AlternateIds.class))).thenReturn(new CallWrapper<>(new SnapshotComplete()));

        check = new ChronopolisCheck(data(), receipts(), bridge,  generator, cleaningManager);
        check.run();
        verify(bagService, times(2)).get(any(Map.class));
        verify(bridge, times(3)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(1)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

    @Test
    public void testIncompleteSnapshot() {
        when(generator.bags()).thenReturn(bagService);
        when(bagService.get(any(Map.class))).thenReturn(new CallWrapper<>(new PageImpl<>
                (ImmutableList.of(toBagModel(createChronBagPartialReplications())))));

        check = new ChronopolisCheck(data(), receipts(), bridge,  generator, cleaningManager);
        check.run();
        verify(bagService, times(2)).get(any(Map.class));
        verify(bridge, times(0)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(0)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

}