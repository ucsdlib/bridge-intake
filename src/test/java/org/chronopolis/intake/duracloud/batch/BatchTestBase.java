package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.model.BagReceipt;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.enums.BagStatus;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Base class for our tests under this package
 * <p>
 * Has basic methods to create test data and holds
 * our mocked interfaces
 * <p>
 * @author shake
 */
public class BatchTestBase {
    private static final String MEMBER = "test-member";
    private static final String NAME = "5309da6f-c1cc-40ad-be42-e67e722cce04";
    private static final String SNAPSHOT_ID = "test-snapshot-id";
    protected static final String DEPOSITOR = "test-depositor";

    protected IntakeSettings settings = new IntakeSettings();
    protected BagStagingProperties stagingProperties = new BagStagingProperties();

    protected BagData data() {
        BagData data = new BagData("");
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

    // Chronopolis Entities

    protected Bag createChronBag(BagStatus status, Set<String> replications) {
        long bits = UUID.randomUUID().getMostSignificantBits();
        return new Bag(bits, bits, bits, null, null, ZonedDateTime.now(), ZonedDateTime.now(),
                NAME, DEPOSITOR, DEPOSITOR, status, replications);
    }

    protected Bag createChronBagPartialReplications() {
        return createChronBag(BagStatus.REPLICATING, ImmutableSet.of(UUID.randomUUID().toString()));
    }

    protected Bag createChronBagFullReplications() {
        return createChronBag(BagStatus.PRESERVED,
                ImmutableSet.of(UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString()));
    }

}
