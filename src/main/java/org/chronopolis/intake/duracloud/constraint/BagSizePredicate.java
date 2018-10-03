package org.chronopolis.intake.duracloud.constraint;

import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.props.Constraints;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Predicate to test that a Bag does not exceed the size limit of a given Node
 *
 * Question: Could we pass in a Map[String, BagSizeLimit] along with a Bag I guess then use the
 * node name from the weight to get the related BagSizeLimit. We then create the Predicate
 * programmatically in the DpnReplicate thread.
 *
 * @author shake
 */
public class BagSizePredicate implements Predicate<Weight> {

    private final Double size;
    private final Map<String, Constraints.SizeLimit> constraintMap;

    public BagSizePredicate(double size, Map<String, Constraints.SizeLimit> constraintMap) {
        this.size = size;
        this.constraintMap = constraintMap;
    }

    @Override
    public boolean test(Weight weight) {
        Constraints.SizeLimit limit = constraintMap.getOrDefault(weight.getNode(),
                new Constraints.SizeLimit());
        //     no limit               check against bag size
        return limit.getSize() < 0 || size < (limit.getSize() * limit.getUnit().size());
    }
}
