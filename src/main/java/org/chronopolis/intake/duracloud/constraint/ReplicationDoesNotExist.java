package org.chronopolis.intake.duracloud.constraint;

import org.chronopolis.intake.duracloud.batch.support.Weight;

import java.util.function.Predicate;

/**
 * Predicate to test that a Node does not already have an active Replication
 *
 * @author shake
 */
public class ReplicationDoesNotExist implements Predicate<Weight> {
    private final String existingNode;

    public ReplicationDoesNotExist(String existingNode) {
        this.existingNode = existingNode;
    }

    @Override
    public boolean test(Weight weight) {
        return !existingNode.equals(weight.getNode());
    }
}
