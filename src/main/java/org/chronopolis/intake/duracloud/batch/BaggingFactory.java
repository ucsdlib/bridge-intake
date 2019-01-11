package org.chronopolis.intake.duracloud.batch;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.batch.bagging.BaggingTasklet;
import org.chronopolis.intake.duracloud.config.BridgeContext;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.model.BagData;
import org.chronopolis.intake.duracloud.notify.Notifier;

/**
 * Same deal as with DpnFactory
 *
 * @author shake
 */
public class BaggingFactory {

    private final Notifier notifier;
    private final BagProperties bagProperties;
    private final BagStagingProperties stagingProperties;

    public BaggingFactory(Notifier notifier,
                          BagProperties bagProperties,
                          BagStagingProperties stagingProperties) {
        this.notifier = notifier;
        this.bagProperties = bagProperties;
        this.stagingProperties = stagingProperties;
    }

    @SuppressWarnings("WeakerAccess")
    public BaggingTasklet baggingTasklet(BagData data, BridgeContext context) {
        return new BaggingTasklet(data.snapshotId(),
                data.depositor(),
                context,
                bagProperties,
                stagingProperties,
                notifier);
    }
}
