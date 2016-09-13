package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.MediaType;
import okhttp3.ResponseBody;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.intake.duracloud.test.TestApplication;
import org.chronopolis.rest.entities.Bag;
import org.chronopolis.rest.entities.BagDistribution;
import org.chronopolis.rest.entities.Node;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import retrofit2.Call;
import retrofit2.Callback;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Base class for our tests under this package
 *
 * Has basic methods to create test data and holds
 * our mocked interfaces
 *
 * Created by shake on 6/2/16.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TestApplication.class)
public class BatchTestBase {
    protected final String MEMBER = "test-member";
    protected final String NAME = "test-name";
    protected final String DEPOSITOR = "test-depositor";
    protected final String SNAPSHOT_ID = "test-snapshot-id";

    @Autowired IntakeSettings settings;

    protected BagData data() {
        BagData data = new BagData();
        data.setMember(MEMBER);
        data.setName(NAME);
        data.setDepositor(DEPOSITOR);
        data.setSnapshotId(SNAPSHOT_ID);
        return data;
    }

    protected BagReceipt receipt() {
        BagReceipt receipt = new BagReceipt();
        receipt.setName(UUID.randomUUID().toString());
        receipt.setReceipt(UUID.randomUUID().toString());
        return receipt;
    }

    protected List<BagReceipt> receipts() {
        return ImmutableList.of(receipt(), receipt());
    }

    // TODO: Split these further? ChronBase // DpnBase

    // Chronopolis Entities
    protected Node createChronNode() {
        return new Node(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    protected Bag createChronBag() {
        Bag b = new Bag(NAME, DEPOSITOR);
        b.setId(UUID.randomUUID().getMostSignificantBits());
        return b;
    }

    protected Bag createChronBagPartialReplications() {
        Bag b = createChronBag();
        b.addDistribution(createDistribution(b));
        return b;
    }

    protected Bag createChronBagFullReplications() {
        Bag b = createChronBag();
        b.addDistribution(createDistribution(b));
        b.addDistribution(createDistribution(b));
        b.addDistribution(createDistribution(b));
        return b;
    }

    protected BagDistribution createDistribution(Bag b) {
        BagDistribution dist = new BagDistribution();
        dist.setBag(b);
        dist.setNode(createChronNode());
        dist.setStatus(BagDistribution.BagDistributionStatus.REPLICATE);
        return dist;
    }

    // DPN Entities
    protected org.chronopolis.earth.models.Bag createBagNoReplications() {
        org.chronopolis.earth.models.Bag b = new org.chronopolis.earth.models.Bag();
        b.setUuid(UUID.randomUUID().toString());
        b.setLocalId("local-id");
        b.setFirstVersionUuid(b.getUuid());
        b.setIngestNode("test-node");
        b.setAdminNode("test-node");
        b.setBagType('D');
        b.setMember(MEMBER);
        b.setCreatedAt(ZonedDateTime.now());
        b.setUpdatedAt(ZonedDateTime.now());
        b.setSize(10L);
        b.setVersion(1L);
        b.setInterpretive(new ArrayList<>());
        b.setReplicatingNodes(new ArrayList<>());
        b.setRights(new ArrayList<>());
        b.setFixities(ImmutableMap.of("fixity-algorithm", "fixity-value"));
        return b;
    }

    protected org.chronopolis.earth.models.Bag createBagFullReplications() {
        org.chronopolis.earth.models.Bag b = createBagNoReplications();
        b.setReplicatingNodes(ImmutableList.of("test-repl-1", "test-repl-2", "test-repl-3"));
        return b;
    }

    protected org.chronopolis.earth.models.Bag createBagPartialReplications() {
        org.chronopolis.earth.models.Bag b = createBagNoReplications();
        b.setReplicatingNodes(ImmutableList.of("test-repl-1"));
        return b;
    }

    // Subclasses to wrap our http calls

    public class CallWrapper<E> implements Call<E> {

        E e;

        public CallWrapper(E e) {
            this.e = e;
        }

        @Override
        public retrofit2.Response<E> execute() throws IOException {
            return retrofit2.Response.success(e);
        }

        @Override
        public void enqueue(Callback<E> callback) {
            callback.onResponse(retrofit2.Response.success(e));
        }

        @Override
        public boolean isExecuted() {
            return false;
        }

        @Override
        public void cancel() {
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public Call<E> clone() {
            return null;
        }
    }

    public class NotFoundWrapper<E> extends CallWrapper<E> {

        public NotFoundWrapper(E e) {
            super(e);
        }

        @Override
        public retrofit2.Response<E> execute() throws IOException {
            return retrofit2.Response.error(404, ResponseBody.create(MediaType.parse("application/json"), ""));
        }

        @Override
        public void enqueue(Callback<E> callback) {
            callback.onResponse(retrofit2.Response.<E>error(404, ResponseBody.create(MediaType.parse("application/json"), "")));
        }

    }
}
