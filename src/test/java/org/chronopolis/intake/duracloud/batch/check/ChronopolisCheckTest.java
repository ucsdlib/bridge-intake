package org.chronopolis.intake.duracloud.batch.check;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.cleaner.TrueCleaner;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.AlternateIds;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.intake.duracloud.remote.model.HistorySummary;
import org.chronopolis.intake.duracloud.remote.model.SnapshotComplete;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.rest.models.Bag;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageImpl;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
        BagData bagData = data();
        BagReceipt receipt = receipt();
        Bag bag = createChronBagFullReplications();
        CallWrapper<PageImpl<Bag>> bagGetWrapper =
                new CallWrapper<>(new PageImpl<>(ImmutableList.of(bag)));
        ImmutableMap<String, Object> params =
                ImmutableMap.of("depositor", bagData.depositor(), "name", receipt.getName());

        // all our mocks...
        // 1. create the bag service
        // 2. GET /api/bags(params)
        // 3. clean staging
        // 4. POST /snapshot/{id}/history
        // 5. POST /snapshot/{id}/complete
        when(generator.bags()).thenReturn(bagService);
        when(bagService.get(eq(params))).thenReturn(bagGetWrapper);
        when(cleaningManager.forChronopolis(eq(bag))).thenReturn(new TrueCleaner());
        when(bridge.postHistory(any(String.class), any(History.class)))
                .thenReturn(new CallWrapper<>(new HistorySummary()));
        when(bridge.completeSnapshot(any(String.class), any(AlternateIds.class)))
                .thenReturn(new CallWrapper<>(new SnapshotComplete()));

        check = new ChronopolisCheck(bagData, ImmutableList.of(receipt), bridge, generator, cleaningManager);
        check.run();

        verify(bagService, times(1)).get(eq(params));
        verify(bridge, times(3)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(1)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

    @Test
    public void testIncompleteSnapshot() {
        when(generator.bags()).thenReturn(bagService);
        when(bagService.get(any(Map.class))).thenReturn(new CallWrapper<>(new PageImpl<>
                (ImmutableList.of(createChronBagPartialReplications()))));

        check = new ChronopolisCheck(data(), receipts(), bridge, generator, cleaningManager);
        check.run();
        verify(bagService, times(2)).get(any(Map.class));
        verify(bridge, times(0)).postHistory(any(String.class), any(History.class));
        verify(bridge, times(0)).completeSnapshot(any(String.class), any(AlternateIds.class));
    }

}