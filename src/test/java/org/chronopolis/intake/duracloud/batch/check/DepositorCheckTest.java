package org.chronopolis.intake.duracloud.batch.check;

import org.chronopolis.earth.api.BalustradeMember;
import org.chronopolis.earth.models.Member;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.models.Depositor;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZonedDateTime;

import static com.google.common.collect.ImmutableSet.of;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DepositorCheckTest {

    private final String empty = "";
    private final String snapshot = "depositor-check-test-snapshot";
    private final String depositor = "depositor-check-test";
    private final ZonedDateTime now = ZonedDateTime.now();
    private final Member dpnMember = new Member(depositor, depositor, depositor);
    private final Depositor chronDepositor =
            new Depositor(1L, depositor, depositor, depositor, now, now, of(), of());

    private final BridgeContext context = new BridgeContext(mock(BridgeAPI.class),
            empty, empty, empty, empty, Push.DPN, empty);

    private final Notifier notifier = mock(Notifier.class);
    private final BalustradeMember dpn = mock(BalustradeMember.class);
    private final DepositorService chronopolis = mock(DepositorService.class);

    @Test
    public void testSuccess() {
        CallWrapper<Member> dpnCall = new CallWrapper<>(dpnMember);
        CallWrapper<Depositor> chronCall = new CallWrapper<>(chronDepositor);

        BagData data = new BagData(empty);
        data.setMember(depositor);
        data.setDepositor(depositor);
        data.setSnapshotId(snapshot);

        DepositorCheck check = new DepositorCheck(notifier, dpn, chronopolis);

        when(chronopolis.getDepositor(eq(depositor))).thenReturn(chronCall);
        when(dpn.getMember(eq(depositor))).thenReturn(dpnCall);

        Assert.assertTrue(check.test(data, context));

        verify(notifier, never()).notify(anyString(), anyString());
    }

    @Test
    public void testFail() {
        String title = "Missing depositor " + depositor;
        String message = "Snapshot Id: " + snapshot
                + "\nChronopolis Depositor " + depositor + " is missing\n"
                + "DPN Member " + depositor + " is missing\n";

        CallWrapper<Member> dpnCall = new ErrorCallWrapper<>(dpnMember, 404, "Not Found");
        CallWrapper<Depositor> chronCall = new ErrorCallWrapper<>(chronDepositor, 404, "Not Found");

        BagData data = new BagData(empty);
        data.setMember(depositor);
        data.setDepositor(depositor);
        data.setSnapshotId(snapshot);

        DepositorCheck check = new DepositorCheck(notifier, dpn, chronopolis);

        when(chronopolis.getDepositor(eq(depositor))).thenReturn(chronCall);
        when(dpn.getMember(eq(depositor))).thenReturn(dpnCall);

        Assert.assertFalse(check.test(data, context));

        verify(notifier, times(1)).notify(eq(title), eq(message));
    }

}
