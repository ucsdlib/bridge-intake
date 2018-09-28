package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.models.Node;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.DPN;
import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;
import org.chronopolis.test.support.CallWrapper;
import org.chronopolis.test.support.ErrorCallWrapper;
import org.chronopolis.test.support.ExceptingCallWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the DpnNodeWeighter
 *
 * Created by shake on 6/5/17.
 */
public class DpnNodeWeighterTest {

    private final String node = "WEIGHT-TEST";

    private DpnNodeWeighter weighter;

    private BalustradeNode nodes = mock(BalustradeNode.class);

    @Before
    public void setup() {
        // Setup our settings
        IntakeSettings settings = new IntakeSettings();
        DPN dpn = new DPN();
        dpn.setUsername(node);
        settings.setDpn(dpn);

        // And setup the snapshot details
        final String id = "TEST-SNAPSHOT-ID";
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId(id);

        weighter = new DpnNodeWeighter(nodes, settings, details);
    }

    private Node response() {
        return new Node().setReplicateTo(ImmutableList.of("test-1", "test-2", "test-3"));
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
        when(nodes.getNode(node)).thenReturn(new ExceptingCallWrapper<>(response()));
        List<Weight> weights = weighter.get();
        Assert.assertEquals(0, weights.size());
        verify(nodes, times(1)).getNode(node);
    }

    @Test
    public void serverError() {
        when(nodes.getNode(node)).thenReturn(new ErrorCallWrapper<>(response(), 404, "not found"));
        List<Weight> weights = weighter.get();
        Assert.assertEquals(0, weights.size());
        verify(nodes, times(1)).getNode(node);
    }

}