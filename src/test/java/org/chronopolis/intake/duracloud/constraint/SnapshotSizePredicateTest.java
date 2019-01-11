package org.chronopolis.intake.duracloud.constraint;

import com.google.common.collect.ImmutableMap;
import org.chronopolis.bag.core.Unit;
import org.chronopolis.intake.duracloud.config.props.Constraints.SizeLimit;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SnapshotSizePredicateTest {

    private final SizeLimit limit = new SizeLimit()
            .setSize(1)
            .setUnit(Unit.KILOBYTE);
    private final String nodeName = "snapshot-size-predicate";
    private final Map<String, SizeLimit> limitMap = ImmutableMap.of(nodeName, limit);

    @Test
    public void testPassNoLimit() {
        ImmutableMap<String, SizeLimit> limitMap = ImmutableMap.of(nodeName, new SizeLimit());
        SnapshotSizePredicate predicate = new SnapshotSizePredicate(4096.0, limitMap);
        Assert.assertTrue(predicate.test(nodeName));
    }

    @Test
    public void testFailNull() {
        SnapshotSizePredicate predicate = new SnapshotSizePredicate(null, limitMap);
        Assert.assertFalse(predicate.test(nodeName));
    }

    @Test
    public void testPassSizeLt() {
        SnapshotSizePredicate predicate = new SnapshotSizePredicate(512.0, limitMap);
        Assert.assertTrue(predicate.test(nodeName));
    }


    @Test
    public void testFailSizeGt() {
        SnapshotSizePredicate predicate = new SnapshotSizePredicate(2048.0, limitMap);
        Assert.assertFalse(predicate.test(nodeName));
    }
}