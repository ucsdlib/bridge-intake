package org.chronopolis.intake.duracloud.constraint;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.chronopolis.intake.duracloud.config.props.Constraints;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Predicate to test that a Snapshot does not exceed the size limit of a given Node's Bag size limit
 * This is to ensure that all Bags in a Snapshot will be able to replicate to a Node.
 *
 * todo We could have this be a BiFunction and pass the SnapshotDetails to the test
 * todo Something other than a String
 *
 * @author shake
 */
public class SnapshotSizePredicate implements Predicate<String> {

    @Nullable private final Double size;
    private final Map<String, Constraints.SizeLimit> constraintMap;

    public SnapshotSizePredicate(Double size, Map<String, Constraints.SizeLimit> constraintMap) {
        this.size = size;
        this.constraintMap = constraintMap;
    }

    @Override
    public boolean test(String node) {
        Constraints.SizeLimit limit = constraintMap.getOrDefault(node, new Constraints.SizeLimit());

        //     no limit
        return limit.getSize() < 0 ||
                // check against bag size
                (size != null && size < (limit.getSize() * limit.getUnit().size()));
    }
}
