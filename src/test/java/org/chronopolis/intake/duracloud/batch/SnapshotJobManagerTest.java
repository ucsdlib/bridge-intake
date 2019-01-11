package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.batch.check.DepositorCheck;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SnapshotJobManagerTest {

    /**
     * Test that when a depositor check fails a snapshot is not added to the processing set
     */
    @Test
    public void testDepositorCheckFail() {
        String empty = "";

        BridgeAPI bridge = mock(BridgeAPI.class);
        DpnFactory dpnFactory = mock(DpnFactory.class);
        ChronFactory chronFactory = mock(ChronFactory.class);
        BaggingFactory bagFactory = mock(BaggingFactory.class);
        DepositorCheck depositorCheck = mock(DepositorCheck.class);

        ThreadPoolExecutor executor
                = new ThreadPoolExecutor(1, 1, 0, MILLISECONDS, new LinkedBlockingQueue<>());
        ConcurrentSkipListSet<String> processing = new ConcurrentSkipListSet<>();

        SnapshotJobManager manager = new SnapshotJobManager(
                dpnFactory, chronFactory, bagFactory, depositorCheck, processing, executor, executor
        );

        BagData bagData = new BagData(empty);
        bagData.setSnapshotId("snapshot");
        bagData.setMember("member");
        bagData.setDepositor("depositor");
        bagData.setName("test-depositor-check-fail");

        BridgeContext context =
                new BridgeContext(bridge, empty, empty, empty, empty, Push.NONE, empty);
        SnapshotDetails details = new SnapshotDetails();

        when(depositorCheck.test(eq(bagData), eq(context))).thenReturn(false);

        manager.startReplicationTasklet(bagData, details, ImmutableList.of(), context);

        Assert.assertTrue(processing.isEmpty());
        verify(depositorCheck, times(1)).test(eq(bagData), eq(context));
        verify(chronFactory, never()).ingest(any(), any(), any());
        verify(chronFactory, never()).check(any(), any(), any());
        verify(dpnFactory, never()).dpnCheck(any(), any(), any());
        verify(dpnFactory, never()).dpnIngest(any(), any(), any());
        verify(dpnFactory, never()).dpnDigest(any(), any());
        verify(dpnFactory, never()).dpnReplicate(any(), any());
    }

}