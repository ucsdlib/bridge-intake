package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.bag.core.Unit;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.models.Node;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.batch.support.ExceptingWrapper;
import org.chronopolis.intake.duracloud.batch.support.NotFoundWrapper;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Constraints;
import org.chronopolis.intake.duracloud.config.props.DPN;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the DpnNodeWeighter
 * <p>
 * Created by shake on 6/5/17.
 */
@SuppressWarnings("FieldCanBeLocal")
public class DpnNodeWeighterTest {

    private final String id = "TEST-SNAPSHOT-ID";
    private final String member = "test-member-id";
    private final String node = "WEIGHT-TEST";

    private final String replicateA = "replicate-to-a";
    private final String replicateB = "replicate-to-b";
    private final String replicateC = "replicate-to-c";

    private SnapshotDetails details;
    private DpnNodeWeighter weighter;

    @Mock
    private BalustradeNode nodes;

    @Before
    public void setup() {
        // Setup our mock api
        nodes = mock(BalustradeNode.class);

        // Setup our settings
        IntakeSettings settings = new IntakeSettings();
        DPN dpn = new DPN();
        dpn.setUsername(node);
        settings.setDpn(dpn);

        // And setup the snapshot details
        details = new SnapshotDetails();
        details.setSnapshotId(id);
        details.setMemberId(member);
        details.setTotalSizeInBytes("1024");

        weighter = new DpnNodeWeighter(nodes, settings, details);
    }

    private Node response() {
        return new Node().setReplicateTo(ImmutableList.of(replicateA, replicateB, replicateC));
    }

    @Test
    public void success() {
        when(nodes.getNode(node)).thenReturn(new CallWrapper<>(response()));
        List<Weight> weights = weighter.get();
        Assert.assertEquals(3, weights.size());
        verify(nodes, times(1)).getNode(node);
    }

    @Test
    public void exception() {
        when(nodes.getNode(node)).thenReturn(new ExceptingWrapper<>());
        List<Weight> weights = weighter.get();
        Assert.assertEquals(0, weights.size());
        verify(nodes, times(1)).getNode(node);
    }

    @Test
    public void serverError() {
        when(nodes.getNode(node)).thenReturn(new NotFoundWrapper<>(response()));
        List<Weight> weights = weighter.get();
        Assert.assertEquals(0, weights.size());
        verify(nodes, times(1)).getNode(node);
    }

    @Test
    public void successWithSizeFilter() {
        DPN dpn = new DPN();
        dpn.setUsername(node);
        IntakeSettings settings = new IntakeSettings();
        settings.setDpn(dpn);
        settings.getConstraints().setNodes(ImmutableList.of(
                new Constraints.Node()
                        .setName(replicateA)
                        .setSizeLimit(new Constraints.SizeLimit()
                                .setSize(1)
                                .setUnit(Unit.BYTE))));
        weighter = new DpnNodeWeighter(nodes, settings, details);

        // test
        when(nodes.getNode(node)).thenReturn(new CallWrapper<>(response()));
        List<Weight> weights = weighter.get();

        verify(nodes, times(1)).getNode(node);
        Assert.assertEquals(2, weights.size());
        boolean noneMatch = weights.stream()
                .noneMatch(weight -> weight.getNode().equals(replicateA));
        Assert.assertTrue(noneMatch);
    }

    @Test
    public void successWithMemberFilter() {
        DPN dpn = new DPN();
        dpn.setUsername(node);
        IntakeSettings settings = new IntakeSettings();
        settings.setDpn(dpn);
        settings.getConstraints().setNodes(ImmutableList.of(new Constraints.Node()
                .setName(replicateB)
                .setMembers(ImmutableList.of(member))));
        weighter = new DpnNodeWeighter(nodes, settings, details);

        // test
        when(nodes.getNode(node)).thenReturn(new CallWrapper<>(response()));
        List<Weight> weights = weighter.get();

        verify(nodes, times(1)).getNode(node);
        Assert.assertEquals(2, weights.size());
        boolean noneMatch = weights.stream()
                .noneMatch(weight -> weight.getNode().equals(replicateB));
        Assert.assertTrue(noneMatch);
    }

}