package org.chronopolis.intake.duracloud.cleaner;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.StagingStorage;
import org.chronopolis.rest.models.enums.BagStatus;
import org.chronopolis.rest.models.update.ActiveToggle;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;

import static com.google.common.collect.ImmutableSet.of;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the Chronopolis Cleaner
 * <p>
 * @author shake
 */
public class ChronopolisCleanerTest {

    private final String BAG_PATH = "BAG";
    private final String BAG_NAME = "test-bag";
    private final String DEPOSITOR = "test-depositor";
    private final ActiveToggle TOGGLE = new ActiveToggle(false);
    private final Path RELATIVE = Paths.get(DEPOSITOR, BAG_NAME);
    private final StagingStorage ACTIVE_STORAGE = generateStorage(true);
    private final StagingStorage INACTIVE_STORAGE = generateStorage(false);

    private BagStagingProperties properties;
    private final StagingService staging = mock(StagingService.class);
    private final DepositorService depositors = mock(DepositorService.class);

    @Before
    public void setup() throws IOException {
        Path tmp = Files.createTempDirectory("cleanertest");
        tmp.toFile().deleteOnExit();
        properties = new BagStagingProperties().setPosix(new Posix().setPath(tmp.toString()));
    }

    private StagingStorage generateStorage(boolean active) {
        return new StagingStorage(active, 1L, 1L, 1L, DEPOSITOR + "/" + BAG_NAME, of());
    }

    private Bag generateBag(StagingStorage storage, BagStatus status) {
        return new Bag(1L, 1L, 1L, storage, storage, ZonedDateTime.now(), ZonedDateTime.now(),
                BAG_NAME, DEPOSITOR, DEPOSITOR, status, of());
    }

    @Test
    public void removeFromBagSuccess() {
        runCleanerForBag(INACTIVE_STORAGE, true);
    }

    @Test
    public void removeFromBagFailDeactivate() {
        runCleanerForBag(ACTIVE_STORAGE, false);
    }

    private void runCleanerForBag(StagingStorage model, Boolean expected) {
        Bag bag = generateBag(model, BagStatus.PRESERVED);
        when(staging.toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(model));

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, depositors, staging, properties, bag);
        Boolean clean = cleaner.call();

        Assert.assertEquals(expected, clean);
        verify(staging, times(1)).toggleStorage(eq(bag.getId()), eq(BAG_PATH), eq(TOGGLE));
    }

    @Test
    public void removeFromQuerySuccess() {
        Bag bag = generateBag(generateStorage(false), BagStatus.PRESERVED);
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
        Bag bag = generateBag(generateStorage(true), BagStatus.PRESERVED);
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
        Bag bag = generateBag(generateStorage(true), BagStatus.PRESERVED);
        ErrorCallWrapper<Bag> wrapper = new ErrorCallWrapper<>(bag, 404, "not-found");
        when(depositors.getDepositorBag(eq(DEPOSITOR), eq(bag.getName()))).thenReturn(wrapper);

        Cleaner cleaner =
                new ChronopolisCleaner(RELATIVE, depositors, staging, properties, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(depositors, times(1)).getDepositorBag(eq(DEPOSITOR), eq(bag.getName()));
        verify(staging, times(0)).toggleStorage(anyLong(), any(), any());
    }

    @Test
    public void removeFromQueryBagNotPreserved() {
        Bag bag = generateBag(generateStorage(true), BagStatus.REPLICATING);
        CallWrapper<Bag> wrapper = new CallWrapper<>(bag);
        when(depositors.getDepositorBag(eq(DEPOSITOR), eq(bag.getName()))).thenReturn(wrapper);

        Cleaner cleaner =
                new ChronopolisCleaner(RELATIVE, depositors, staging, properties, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(depositors, times(1)).getDepositorBag(eq(DEPOSITOR), eq(bag.getName()));
        verify(staging, times(0)).toggleStorage(anyLong(), any(), any());
    }

}