package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.rest.api.DepositorAPI;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.chronopolis.rest.models.storage.ActiveToggle;
import org.chronopolis.rest.models.storage.StagingStorageModel;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the Chronopolis Cleaner
 * <p>
 */
public class ChronopolisCleanerTest {

    private Bag bag;
    private final String BAG_PATH = "BAG";
    private final String BAG_NAME = "test-bag";
    private final String DEPOSITOR = "test-depositor";
    private final ActiveToggle TOGGLE = new ActiveToggle(false);
    private final Path RELATIVE = Paths.get(DEPOSITOR, BAG_NAME);
    private final StagingStorageModel ACTIVE_STORAGE = new StagingStorageModel().setActive(true);
    private final StagingStorageModel INACTIVE_STORAGE = new StagingStorageModel().setActive(false);

    private BagStagingProperties properties;

    private final DepositorAPI depositors = mock(DepositorAPI.class);
    private final StagingService staging = mock(StagingService.class);

    @Before
    public void setup() throws IOException {
        bag = new Bag()
                .setId(1L)
                .setName(BAG_NAME)
                .setDepositor(DEPOSITOR)
                .setBagStorage(new StagingStorageModel()
                        .setActive(true)
                        .setPath(BAG_NAME + DEPOSITOR))
                .setStatus(BagStatus.PRESERVED);

        Path tmp = Files.createTempDirectory("cleanertest");
        properties = new BagStagingProperties().setPosix(new Posix().setPath(tmp.toString()));
    }

    @Test
    public void removeFromBagSuccess() {
        runCleanerForBag(INACTIVE_STORAGE, true);
    }

    @Test
    public void removeFromBagFailDeactivate() {
        runCleanerForBag(ACTIVE_STORAGE, false);
    }

    private void runCleanerForBag(StagingStorageModel model, Boolean expected) {
        when(staging.toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(model));

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, depositors, staging, properties, bag);
        Boolean clean = cleaner.call();

        Assert.assertEquals(expected, clean);
        verify(staging, times(1)).toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE));
    }

    @Test
    public void removeFromQuerySuccess() {
        CallWrapper<Bag> wrapper = new CallWrapper<>(bag);

        when(depositors.getDepositorBag(eq(DEPOSITOR), eq(bag.getName()))).thenReturn(wrapper);
        when(staging.toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(INACTIVE_STORAGE));

        Cleaner cleaner =
                new ChronopolisCleaner(RELATIVE, depositors, staging, properties, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertTrue(clean);
        verify(depositors, times(1)).getDepositorBag(eq(DEPOSITOR), eq(bag.getName()));
        verify(staging, times(1)).toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE));
    }

    @Test
    public void removeFromQueryFailDeactivate() {
        CallWrapper<Bag> wrapper = new CallWrapper<>(bag);

        when(depositors.getDepositorBag(eq(DEPOSITOR), eq(bag.getName()))).thenReturn(wrapper);
        when(staging.toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(ACTIVE_STORAGE));

        Cleaner cleaner
                = new ChronopolisCleaner(RELATIVE, depositors, staging, properties, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(depositors, times(1)).getDepositorBag(eq(DEPOSITOR), eq(bag.getName()));
        verify(staging, times(1)).toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE));
    }

    @Test
    public void removeFailQuery() {
        ErrorCallWrapper<Bag> wrapper = new ErrorCallWrapper<>(bag, 404, "not-found");
        when(depositors.getDepositorBag(eq(DEPOSITOR), eq(bag.getName()))).thenReturn(wrapper);

        Cleaner cleaner =
                new ChronopolisCleaner(RELATIVE, depositors, staging, properties, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(depositors, times(1)).getDepositorBag(eq(DEPOSITOR), eq(bag.getName()));
        verify(staging, times(0)).toggleStorage(any(), any(), any());
    }

    @Test
    public void removeFromQueryBagNotPreserved() {
        bag.setStatus(BagStatus.REPLICATING);
        CallWrapper<Bag> wrapper = new CallWrapper<>(bag);
        when(depositors.getDepositorBag(eq(DEPOSITOR), eq(bag.getName()))).thenReturn(wrapper);

        Cleaner cleaner =
                new ChronopolisCleaner(RELATIVE, depositors, staging, properties, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(depositors, times(1)).getDepositorBag(eq(DEPOSITOR), eq(bag.getName()));
        verify(staging, times(0)).toggleStorage(any(), any(), any());
    }

}