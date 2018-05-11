package org.chronopolis.intake.duracloud.batch.ingest;

import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.earth.models.Digest;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.chronopolis.test.support.ExceptingCallWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class DpnDigestTest extends BatchTestBase {

    @Mock
    private BalustradeBag bags;

    private DpnDigest digest;

    @Before
    public void setup() {
        bags = mock(BalustradeBag.class);

        BagReceipt receipt = receipt();

        digest = new DpnDigest(receipt, bags, settings);
    }

    @Test
    public void saveSuccess() {
        Digest saved = new Digest();
        Bag b = createBagNoReplications();

        when(bags.createDigest(anyString(), any())).thenReturn(new CallWrapper<>(saved));

        digest.apply(b);
        verify(bags, times(1)).createDigest(anyString(), any());
    }

    @Test
    public void saveConflict() {
        Digest saved = new Digest();
        Bag b = createBagNoReplications();

        when(bags.createDigest(anyString(), any()))
                .thenReturn(new ErrorCallWrapper<>(saved, 409, "conflict"));

        digest.apply(b);
        verify(bags, times(1)).createDigest(anyString(), any());
    }

    @Test(expected = RuntimeException.class)
    public void saveFail() {
        Digest saved = new Digest();
        Bag b = createBagNoReplications();

        when(bags.createDigest(anyString(), any()))
                .thenReturn(new ErrorCallWrapper<>(saved, 404, "not found"));

        digest.apply(b);
        verify(bags, times(1)).createDigest(anyString(), any());
    }

    @Test(expected = RuntimeException.class)
    public void httpException() {
        Digest saved = new Digest();
        Bag b = createBagNoReplications();
        when(bags.createDigest(anyString(), any()))
                .thenReturn(new ExceptingCallWrapper<>(saved));

        digest.apply(b);
        verify(bags, times(1)).createDigest(anyString(), any());
    }


}