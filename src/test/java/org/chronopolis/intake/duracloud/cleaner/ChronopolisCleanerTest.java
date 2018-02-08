package org.chronopolis.intake.duracloud.cleaner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.RepairService;
import org.chronopolis.rest.api.ReplicationService;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.api.StorageService;
import org.chronopolis.rest.api.TokenService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.chronopolis.rest.models.storage.ActiveToggle;
import org.chronopolis.rest.models.storage.StagingStorageModel;
import org.chronopolis.test.support.CallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.PageImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
    private final String BAG_NAME = "test-bag";
    private final String DEPOSITOR = "test-depositor";
    private final Map<String, Object> PARAMS =
            ImmutableMap.of("depositor", DEPOSITOR, "name", BAG_NAME);
    private final ActiveToggle TOGGLE = new ActiveToggle(false);
    private final Path RELATIVE = Paths.get(DEPOSITOR, BAG_NAME);
    private final StagingStorageModel ACTIVE_STORAGE = new StagingStorageModel().setActive(true);
    private final StagingStorageModel INACTIVE_STORAGE = new StagingStorageModel().setActive(false);

    private BagStagingProperties properties;
    private final ServiceGenerator GENERATOR = new MockGenerator();

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
        when(GENERATOR.staging().toggleStorage(eq(bag.getId()), eq("BAG"), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(model));

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, properties, GENERATOR, bag);
        Boolean clean = cleaner.call();

        Assert.assertEquals(expected, clean);
        verify(GENERATOR.staging(), times(1)).toggleStorage(eq(bag.getId()), eq("BAG"), eq(TOGGLE));
    }

    @Test
    public void removeFromQuerySuccess() {
        CallWrapper<PageImpl<Bag>> wrapper = new CallWrapper<>(
                new PageImpl<>(ImmutableList.of(bag)));

        when(GENERATOR.bags().get(PARAMS)).thenReturn(wrapper);
        when(GENERATOR.staging().toggleStorage(eq(bag.getId()), eq("BAG"), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(INACTIVE_STORAGE));

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, properties, GENERATOR, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertTrue(clean);
        verify(GENERATOR.bags(), times(1)).get(PARAMS);
        verify(GENERATOR.staging(), times(1)).toggleStorage(eq(bag.getId()), eq("BAG"), eq(TOGGLE));
    }

    @Test
    public void removeFromQueryFailDeactivate() {
        CallWrapper<PageImpl<Bag>> wrapper = new CallWrapper<>(
                new PageImpl<>(ImmutableList.of(bag)));

        when(GENERATOR.bags().get(PARAMS)).thenReturn(wrapper);
        when(GENERATOR.staging().toggleStorage(eq(bag.getId()), eq("BAG"), eq(TOGGLE)))
                .thenReturn(new CallWrapper<>(ACTIVE_STORAGE));

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, properties, GENERATOR, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(GENERATOR.bags(), times(1)).get(PARAMS);
        verify(GENERATOR.staging(), times(1)).toggleStorage(eq(bag.getId()), eq("BAG"), eq(TOGGLE));
    }

    @Test
    public void removeFailQuery() {
        CallWrapper<PageImpl<Bag>> wrapper = new CallWrapper<>(
                new PageImpl<>(ImmutableList.of(bag, bag)));
        when(GENERATOR.bags().get(PARAMS)).thenReturn(wrapper);

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, properties, GENERATOR, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(GENERATOR.bags(), times(1)).get(PARAMS);
        verify(GENERATOR.staging(), times(0)).toggleStorage(any(), any(), any());
    }

    @Test
    public void removeFromQueryBagNotPreserved() {
        bag.setStatus(BagStatus.REPLICATING);
        CallWrapper<PageImpl<Bag>> wrapper = new CallWrapper<>(
                new PageImpl<>(ImmutableList.of(bag)));
        when(GENERATOR.bags().get(PARAMS)).thenReturn(wrapper);

        Cleaner cleaner = new ChronopolisCleaner(RELATIVE, properties, GENERATOR, DEPOSITOR, BAG_NAME);
        Boolean clean = cleaner.call();

        Assert.assertFalse(clean);
        verify(GENERATOR.bags(), times(1)).get(PARAMS);
        verify(GENERATOR.staging(), times(0)).toggleStorage(any(), any(), any());
    }

    public class MockGenerator implements ServiceGenerator {

        private final BagService bags = mock(BagService.class);
        private final StagingService staging = mock(StagingService.class);

        @Override
        public BagService bags() {
            return bags;
        }

        @Override
        public TokenService tokens() {
            return null;
        }

        @Override
        public RepairService repairs() {
            return null;
        }

        @Override
        public StagingService staging() {
            return staging;
        }

        @Override
        public StorageService storage() {
            return null;
        }

        @Override
        public ReplicationService replications() {
            return null;
        }
    }

}