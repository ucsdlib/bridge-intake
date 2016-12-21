package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.IngestRequest;
import org.chronopolis.rest.support.BagConverter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * Created by shake on 6/2/16.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
public class ChronopolisIngestTest extends BatchTestBase {

    @Mock IngestAPI api;

    ChronopolisIngest ingest;

    @Before
    public void setup() {
        settings.setPushChronopolis(true);
        MockitoAnnotations.initMocks(this);

        ingest = new ChronopolisIngest(data(), receipts(), api, settings);
    }

    @Test
    public void run() throws Exception {
        when(api.stageBag(any(IngestRequest.class))).thenReturn(new CallWrapper<Bag>(BagConverter.toBagModel(createChronBag())));
        ingest.run();

        verify(api, times(2)).stageBag(any(IngestRequest.class));
    }

}