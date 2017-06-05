package org.chronopolis.intake.duracloud.batch;

import com.google.common.collect.ImmutableList;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Node;
import org.chronopolis.intake.duracloud.batch.support.CallWrapper;
import org.chronopolis.intake.duracloud.batch.support.ExceptingWrapper;
import org.chronopolis.intake.duracloud.batch.support.NotFoundWrapper;
import org.chronopolis.intake.duracloud.batch.support.Weight;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.Chron;
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
 *
 * Created by shake on 6/5/17.
 */
public class DpnNodeWeighterTest {

    private final String id = "TEST-SNAPSHOT-ID";
    private final String node = "WEIGHT-TEST";

    private DpnNodeWeighter weighter;

    @Mock
    private BalustradeNode nodes;

    @Before
    public void setup() {
        // Setup our mock api
        nodes = mock(BalustradeNode.class);
        LocalAPI api = new LocalAPI();
        api.setNodeAPI(nodes);

        // Setup our settings
        IntakeSettings settings = new IntakeSettings();
        Chron chron = new Chron();
        chron.setNode(node);
        settings.setChron(chron);

        // And setup the snapshot details
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId(id);

        weighter = new DpnNodeWeighter(api, settings, details);
    }

    private Node response() {
        return new Node().setReplicateTo(ImmutableList.of("test-1", "test-2", "test-3"));
    }

    @Test
    public void success() throws Exception {
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

}