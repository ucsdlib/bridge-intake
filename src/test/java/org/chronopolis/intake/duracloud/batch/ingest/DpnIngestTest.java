package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.collect.ImmutableList;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.models.Bag;
import org.chronopolis.intake.duracloud.DpnInfoReader;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
public class DpnIngestTest extends BatchTestBase {

    private DpnIngest ingest;
    private final Long VERSION = 1L;
    private final String LOCAL_ID = "dpn-ingest-test";

    // Mocks
    @Mock private BalustradeBag bags;
    @Mock private DpnInfoReader dpnInfoReader;
    @Mock private DpnIngest.ReaderFactory readerFactory;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        BagData data = data();
        BagReceipt receipt = receipt();
        ingest = new DpnIngest(data, receipt, bags, settings, stagingProperties, readerFactory);
    }

    @Test
    public void bagExists() {
        Bag b = createBagNoReplications();

        when(bags.getBag(anyString())).thenReturn(new CallWrapper<>(b));
        Bag bag = ingest.get();

        Assert.assertEquals(b, bag);
        verify(bags, times(1)).getBag(anyString());
    }

    @Test(expected = RuntimeException.class)
    public void readerException() throws IOException {
        Bag b = createBagNoReplications();

        when(bags.getBag(anyString())).thenReturn(new ErrorCallWrapper<>(b, 404, "Not Found"));
        when(readerFactory.reader(any(Path.class), anyString()))
                .thenThrow(new IOException("Unable to read file"));

        ingest.get();
        verify(bags, times(1)).getBag(anyString());
        verify(readerFactory, times(1)).reader(any(), anyString());
    }


    @Test
    public void registerBagSuccess() throws IOException {
        Bag b = createBagNoReplications();

        when(bags.getBag(anyString())).thenReturn(new ErrorCallWrapper<>(b, 404, "Not Found"));
        when(readerFactory.reader(any(Path.class), anyString())).thenReturn(dpnInfoReader);
        when(dpnInfoReader.getLocalId()).thenReturn(LOCAL_ID);
        when(dpnInfoReader.getVersionNumber()).thenReturn(VERSION);
        when(dpnInfoReader.getFirstVersionUUID()).thenReturn(LOCAL_ID);
        when(dpnInfoReader.getRightsIds()).thenReturn(ImmutableList.of());
        when(dpnInfoReader.getInterpretiveIds()).thenReturn(ImmutableList.of());

        // would be nice to have this match with the properties we just gave
        when(bags.createBag(any())).thenReturn(new CallWrapper<>(b));
        Bag bag = ingest.get();

        Assert.assertEquals(b, bag);
        verify(bags, times(1)).getBag(anyString());
        verify(readerFactory, times(1)).reader(any(), anyString());
        verify(dpnInfoReader, times(1)).getLocalId();
        verify(dpnInfoReader, times(1)).getRightsIds();
        verify(dpnInfoReader, times(1)).getVersionNumber();
        verify(dpnInfoReader, times(1)).getInterpretiveIds();
        verify(dpnInfoReader, times(1)).getFirstVersionUUID();
    }

    @Test(expected = RuntimeException.class)
    public void registerBagError() throws IOException {
        Bag b = createBagNoReplications();

        when(bags.getBag(anyString())).thenReturn(new ErrorCallWrapper<>(b, 404, "Not Found"));
        when(readerFactory.reader(any(Path.class), anyString())).thenReturn(dpnInfoReader);
        when(dpnInfoReader.getLocalId()).thenReturn(LOCAL_ID);
        when(dpnInfoReader.getVersionNumber()).thenReturn(VERSION);
        when(dpnInfoReader.getFirstVersionUUID()).thenReturn(LOCAL_ID);
        when(dpnInfoReader.getRightsIds()).thenReturn(ImmutableList.of());
        when(dpnInfoReader.getInterpretiveIds()).thenReturn(ImmutableList.of());

        // would be nice to have this match with the properties we just gave
        when(bags.createBag(any())).thenReturn(new ErrorCallWrapper<>(b, 401, "Unauthorized"));

        ingest.get();
        verify(bags, times(1)).getBag(anyString());
        verify(readerFactory, times(1)).reader(any(), anyString());
        verify(dpnInfoReader, times(1)).getLocalId();
        verify(dpnInfoReader, times(1)).getRightsIds();
        verify(dpnInfoReader, times(1)).getVersionNumber();
        verify(dpnInfoReader, times(1)).getInterpretiveIds();
        verify(dpnInfoReader, times(1)).getFirstVersionUUID();
    }

}