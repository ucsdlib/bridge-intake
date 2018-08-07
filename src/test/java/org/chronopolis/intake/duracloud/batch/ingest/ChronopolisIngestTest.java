package org.chronopolis.intake.duracloud.batch.ingest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.chronopolis.intake.duracloud.batch.BatchTestBase;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.StagingService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.IngestRequest;
import org.chronopolis.rest.service.IngestRequestSupplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.PageImpl;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import retrofit2.Call;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * todo: we need to verify the fixity value is created in certain cases
 * unfortunately at the moment the ChronopolisIngest class simply tries to read the tagmanifest
 * off disk which makes it hard to check... unless we can generate it. That would require
 * some more setup and basically this is something we need to dig a bit more in to.
 * <p>
 * Created by shake on 6/2/16.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
public class ChronopolisIngestTest extends BatchTestBase {

    @Mock BagService bags;
    @Mock StagingService staging;
    @Mock IngestRequestSupplier supplier;
    @Mock ChronopolisIngest.IngestSupplierFactory factory;

    private BagData data;
    private String prefix = "prefix-";

    @Before
    public void setup() {
        settings.setPushChronopolis(true);
        ingestProperties.setReplicateTo(ImmutableList.of("test-node"));
        MockitoAnnotations.initMocks(this);
        data = data();
    }

    @Test
    public void withoutPrefix() throws Exception {
        settings.setPushDPN(true);
        Bag bag = createChronBag();
        BagReceipt receipt = receipt();
        when(bags.get(eq(
                ImmutableMap.of(
                        "depositor", data.depositor(),
                        "name", receipt.getName()))))
                .thenReturn(getNoBag());
        ChronopolisIngest ingest = new ChronopolisIngest(data, ImmutableList.of(receipt), bags,
                staging, settings, stagingProperties, factory, ingestProperties);

        IngestRequest request = request(receipt, data.depositor(), ".tar");
        when(factory.supplier(
                any(Path.class),
                any(Path.class),
                eq(data.depositor()),
                eq(receipt.getName()))).thenReturn(supplier);
        when(supplier.get()).thenReturn(Optional.of(request));
        when(bags.deposit(eq(request))).thenReturn(new CallWrapper<Bag>(bag));

        ingest.run();

        verify(factory, times(1)).supplier(any(Path.class),
                any(Path.class),
                eq(data.depositor()),
                eq(receipt.getName()));
        verify(supplier, times(1)).get();
        verify(bags, times(1)).deposit(eq(request));
    }

    @Test
    public void withPrefix() throws Exception {
        settings.setPushDPN(false);
        settings.getChron().setPrefix(prefix);

        Bag bag = createChronBag();
        BagData data = data();
        BagReceipt receipt = receipt();
        String depositor = data.depositor();

        when(bags.get(
                eq(ImmutableMap.of(
                        "depositor", depositor,
                        "name", receipt.getName()))))
                .thenReturn(getNoBag());
        ChronopolisIngest ingest = new ChronopolisIngest(data, ImmutableList.of(receipt), bags,
                staging, settings, stagingProperties, factory, ingestProperties);

        IngestRequest request = request(receipt, depositor, null);

        when(factory.supplier(
                any(Path.class),
                any(Path.class),
                eq(depositor),
                eq(receipt.getName()))).thenReturn(supplier);
        when(supplier.get()).thenReturn(Optional.of(request));
        when(bags.deposit(eq(request))).thenReturn(new CallWrapper<>(bag));

        ingest.run();

        verify(factory, times(1)).supplier(
                any(Path.class),
                any(Path.class),
                eq(depositor),
                eq(receipt.getName()));
        verify(supplier, times(1)).get();
        verify(bags, times(1)).deposit(eq(request));

        settings.getChron().setPrefix("");
    }

    @Test
    public void withBagExists() {
        settings.setPushDPN(false);
        Bag bag = createChronBag();
        BagReceipt receipt = receipt();
        when(bags.get(eq(
                ImmutableMap.of(
                        "depositor", data.depositor(),
                        "name", receipt.getName()))))
                .thenReturn(getBagExists(bag));

        verify(factory, times(0)).supplier(
                any(Path.class),
                any(Path.class),
                anyString(),
                anyString());
        verify(supplier, times(0)).get();
        verify(bags, times(0)).deposit(any(IngestRequest.class));
    }

    private Call<PageImpl<Bag>> getNoBag() {
        return new CallWrapper<>(new PageImpl<Bag>(ImmutableList.of()));
    }

    private IngestRequest request(BagReceipt receipt, String depostior, String fileType) {
        String bag = fileType == null ? receipt.getName() : receipt.getName() + fileType;
        Path location = Paths.get(stagingProperties.getPosix().getPath(), data.depositor(), bag);
        List<String> nodes = ingestProperties.getReplicateTo();
        IngestRequest request = new IngestRequest();
        request.setSize(1L);
        request.setTotalFiles(1L);
        request.setDepositor(depostior);
        request.setName(receipt.getName());
        request.setLocation(location.toString());
        request.setReplicatingNodes(nodes);
        request.setRequiredReplications(nodes.size());
        request.setStorageRegion(stagingProperties.getPosix().getId());
        return request;
    }

    public Call<PageImpl<Bag>> getBagExists(Bag bag) {
        return new CallWrapper<>(new PageImpl<Bag>(ImmutableList.of(bag)));
    }
}