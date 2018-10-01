package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.chronopolis.common.storage.Posix;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.Push;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.DepositorService;
import org.chronopolis.rest.api.FileService;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.StagingStorage;
import org.chronopolis.rest.models.create.BagCreate;
import org.chronopolis.rest.models.enums.BagStatus;
import org.chronopolis.rest.models.enums.FixityAlgorithm;
import org.chronopolis.rest.service.BagFileCsvGenerator;
import org.chronopolis.rest.service.BagFileCsvResult;
import org.chronopolis.rest.service.IngestRequestSupplier;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Sup. Tests for {@link ChronopolisIngest}
 * <p>
 * Created by shake on 6/2/16.
 */
public class ChronopolisIngestTest extends BatchTestBase {

    // Path to a file which we can read
    private final String filePath =
            "5309da6f-c1cc-40ad-be42-e67e722cce04/.collection-snapshot.properties";

    private final BagService bags = mock(BagService.class);
    private final StagingService staging = mock(StagingService.class);
    private final FileService fileService = mock(FileService.class);
    private final DepositorService depositorService = mock(DepositorService.class);
    private final BagFileCsvGenerator generator = mock(BagFileCsvGenerator.class);
    private final IngestRequestSupplier supplier = mock(IngestRequestSupplier.class);
    private final ChronopolisIngest.IngestSupplierFactory factory = new ChronIngestFactory();
    private final BridgeContext bridgeContext = new BridgeContext(mock(BridgeAPI.class),
             "", "manifest-sha256", "restores", "snapshots", Push.CHRONOPOLIS);


    private BagData data;

    @Before
    public void setup() {
        data = data();
    }

    @Test
    public void depositBagNoPrefix() {
        BagReceipt receipt = receipt();
        BagCreate create = new BagCreate();
        Bag bag = createChronBag(BagStatus.DEPOSITED, ImmutableSet.of());

        when(depositorService.getDepositorBag(eq(data.depositor()), eq(receipt.getName())))
                .thenReturn(new ErrorCallWrapper<>(null, 404, "bag-not-found"));
        when(supplier.get()).thenReturn(Optional.of(create));
        when(bags.deposit(eq(create))).thenReturn(new CallWrapper<>(bag));
        // halt
        when(generator.call()).thenReturn(new BagFileCsvResult(new IOException("ioexception")));

        ChronopolisIngest ingest = new ChronopolisIngest(data, ImmutableList.of(receipt), bags,
                fileService, staging, depositorService, settings, stagingProperties, bridgeContext,
                factory);
        ingest.run();

        verify(supplier, times(1)).get();
        verify(generator, times(1)).call();
        verify(bags, times(1)).deposit(any());
        verify(depositorService, times(1)).getDepositorBag(anyString(), anyString());

        verify(fileService, never()).createBatch(eq(bag.getId()), any());
        verify(staging, never()).createStorageForBag(anyLong(), anyString(), any());
    }

    @Test
    public void registerFiles() throws URISyntaxException {
        BagReceipt receipt = receipt();
        Bag bag = createChronBag(BagStatus.DEPOSITED, ImmutableSet.of());

        URL bagsResource = ClassLoader.getSystemClassLoader().getResource("bags");
        Assert.assertNotNull(bagsResource);
        Path aFile = Paths.get(bagsResource.toURI()).resolve(filePath);
        CallWrapper<Bag> bagWrap = new CallWrapper<>(bag);
        CallWrapper<Void> voidWrap = new CallWrapper<>(null);
        BagFileCsvResult csvResult = new BagFileCsvResult(aFile);

        when(depositorService.getDepositorBag(
                eq(data.depositor()),
                eq(receipt.getName()))).thenReturn(bagWrap);
        when(generator.call()).thenReturn(csvResult);
        when(fileService.createBatch(eq(bag.getId()), any())).thenReturn(voidWrap);

        ChronopolisIngest ingest = new ChronopolisIngest(data, ImmutableList.of(receipt), bags,
                fileService, staging, depositorService, settings, stagingProperties, bridgeContext,
                factory);
        ingest.run();

        verify(supplier, never()).get();
        verify(bags, never()).deposit(any());
        verify(staging, never()).createStorageForBag(anyLong(), anyString(), any());
        verify(depositorService, times(1)).getDepositorBag(anyString(), anyString());
        verify(generator, times(1)).call();
        verify(fileService, times(1)).createBatch(eq(bag.getId()), any());
    }

    @Test
    public void registerStaging() {
        // make sure we can find our bag :D
        URL bagsResource = ClassLoader.getSystemClassLoader().getResource("bags");
        Assert.assertNotNull(bagsResource);
        stagingProperties.setPosix(new Posix()
                .setPath(bagsResource.getPath())
        );

        BagReceipt receipt = receipt();
        Bag bag = createChronBag(BagStatus.INITIALIZED, ImmutableSet.of());
        StagingStorage storage = new StagingStorage(true, bag.getSize(), 1L, bag.getTotalFiles(),
                filePath, ImmutableSet.of());

        CallWrapper<Bag> bagWrap = new CallWrapper<>(bag);
        CallWrapper<StagingStorage> storageWrap = new CallWrapper<>(storage);

        when(depositorService.getDepositorBag(
                eq(data.depositor()),
                eq(receipt.getName()))).thenReturn(bagWrap);

        when(staging.createStorageForBag(
                eq(bag.getId()),
                anyString(),
                any())).thenReturn(storageWrap);

        ChronopolisIngest ingest = new ChronopolisIngest(data, ImmutableList.of(receipt), bags,
                fileService, staging, depositorService, settings, stagingProperties, bridgeContext,
                factory);
        ingest.run();

        verify(supplier, never()).get();
        verify(generator, never()).call();
        verify(bags, never()).deposit(any());
        verify(fileService, never()).createBatch(eq(bag.getId()), any());

        verify(staging, times(1)).createStorageForBag(eq(bag.getId()), anyString(), any());
        verify(depositorService, times(1)).getDepositorBag(anyString(), anyString());
    }

    @Test
    public void depositBagFail() {
        BagReceipt receipt = receipt();
        Bag bag = createChronBag(BagStatus.DEPOSITED, ImmutableSet.of());
        BagCreate create = new BagCreate();
        CallWrapper<Bag> notFound = new ErrorCallWrapper<>(null, 404, "bag-not-found");
        CallWrapper<Bag> badRequest = new ErrorCallWrapper<>(null, 400, "bad-request");

        when(depositorService.getDepositorBag(
                eq(data.depositor()),
                eq(receipt.getName()))).thenReturn(notFound);

        when(supplier.get()).thenReturn(Optional.of(create));
        when(bags.deposit(eq(create))).thenReturn(badRequest);

        ChronopolisIngest ingest = new ChronopolisIngest(data, ImmutableList.of(receipt), bags,
                fileService, staging, depositorService, settings, stagingProperties, bridgeContext,
                factory);
        ingest.run();

        verify(supplier, times(1)).get();
        verify(bags, times(1)).deposit(any());
        verify(depositorService, times(1)).getDepositorBag(anyString(), anyString());

        verify(generator, never()).call();
        verify(fileService, never()).createBatch(eq(bag.getId()), any());
        verify(staging, never()).createStorageForBag(anyLong(), anyString(), any());
    }

    private class ChronIngestFactory extends ChronopolisIngest.IngestSupplierFactory {

        @Override
        public BagFileCsvGenerator generator(Path output, Path root, FixityAlgorithm algorithm) {
            return generator;
        }

        @Override
        public IngestRequestSupplier supplier(Path location, Path stage, String depositor, String name) {
            return supplier;
        }
    }
}