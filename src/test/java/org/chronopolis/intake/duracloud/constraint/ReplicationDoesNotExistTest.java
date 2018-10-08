package org.chronopolis.intake.duracloud.constraint;

import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.junit.Assert;
import org.junit.Test;

public class ReplicationDoesNotExistTest {

    private final String snapshot = "snapshot-id";
    private final String existingReplication = "existing-replication";
    private final ReplicationDoesNotExist predicate =
            new ReplicationDoesNotExist(existingReplication);

    @Test
    public void testPass() {
        Weight weight = new Weight("new-replication", snapshot);
        Assert.assertTrue(predicate.test(weight));
    }

    @Test
    public void testFail() {
        Weight weight = new Weight(existingReplication, snapshot);
        Assert.assertFalse(predicate.test(weight));
    }
}