package org.chronopolis.intake.duracloud.constraint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MemberToNodePredicateTest {

    private final String testNode = "member-predicate-test";
    private final String testMemberToFilter = "member-should-be-filtered";
    private final Map<String, List<String>> filterMap =
            ImmutableMap.of(testNode, ImmutableList.of(testMemberToFilter));

    @Test
    public void testPass() {
        String testMemberPass = "member-ok";
        MemberToNodePredicate predicate = new MemberToNodePredicate(testMemberPass, filterMap);
        Assert.assertTrue(predicate.test(testNode));
    }

    @Test
    public void testFail() {
        MemberToNodePredicate predicate = new MemberToNodePredicate(testMemberToFilter, filterMap);
        Assert.assertFalse(predicate.test(testNode));
    }
}