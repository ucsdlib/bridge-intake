package org.chronopolis.intake.duracloud.constraint;

import com.google.common.collect.ImmutableList;
import org.chronopolis.intake.duracloud.batch.support.Weight;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Predicate to test that a snapshot member IS NOT contained within a given Set.
 *
 * @author shake
 */
public class MemberToNodePredicate implements Predicate<Weight> {

    private final String member;
    private final Map<String, List<String>> constraintMap;

    public MemberToNodePredicate(String member,
                                 Map<String, List<String>> constraintMap) {
        this.member = member;
        this.constraintMap = constraintMap;
    }

    @Override
    public boolean test(Weight weight) {
        List<String> filter = constraintMap.getOrDefault(weight.getNode(), ImmutableList.of());
        return !filter.contains(member);
    }
}
