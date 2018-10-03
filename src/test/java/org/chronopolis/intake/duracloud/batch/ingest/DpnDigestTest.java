package org.chronopolis.intake.duracloud.batch.ingest;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.chronopolis.test.support.ExceptingCallWrapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DpnIngest}
 *
 * @author shake
 */
public class DpnDigestTest extends BatchTestBase {

    @Rule
    public final ExpectedException expected = ExpectedException.none();

    private final BalustradeBag bags = mock(BalustradeBag.class);
    private final BridgeContext context = new BridgeContext(mock(BridgeAPI.class),
            "prefix", "manifest", "restores", "snapshots", Push.DPN, "short-name");
    private final DpnDigest digest = new DpnDigest(receipt(), context, bags, settings);

    @Test
    public void saveSuccess() {
        Digest saved = new Digest();
        Bag bag = createBagNoReplications();

        when(bags.createDigest(anyString(), any())).thenReturn(new CallWrapper<>(saved));

        digest.apply(bag);
        verify(bags, times(1)).createDigest(anyString(), any());
    }

    @Test
    public void saveConflict() {
        Digest saved = new Digest();
        Bag bag = createBagNoReplications();

        when(bags.createDigest(anyString(), any()))
                .thenReturn(new ErrorCallWrapper<>(saved, 409, "conflict"));

        digest.apply(bag);
        verify(bags, times(1)).createDigest(anyString(), any());
    }

    @Test
    public void saveFail() {
        Digest saved = new Digest();
        Bag bag = createBagNoReplications();

        when(bags.createDigest(anyString(), any()))
                .thenReturn(new ErrorCallWrapper<>(saved, 404, "not found"));

        expected.expect(RuntimeException.class);
        digest.apply(bag);
        verify(bags, times(1)).createDigest(anyString(), any());
    }

    @Test
    public void httpException() {
        Digest saved = new Digest();
        Bag bag = createBagNoReplications();
        when(bags.createDigest(anyString(), any()))
                .thenReturn(new ExceptingCallWrapper<>(saved));

        expected.expect(RuntimeException.class);
        digest.apply(bag);
        verify(bags, times(1)).createDigest(anyString(), any());
    }


}