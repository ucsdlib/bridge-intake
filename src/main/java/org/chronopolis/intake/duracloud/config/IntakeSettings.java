package org.chronopolis.intake.duracloud.config;

import org.chronopolis.common.settings.ChronopolisSettings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by shake on 8/1/14.
 */
@Component
@SuppressWarnings("UnusedDeclaration")
public class IntakeSettings extends ChronopolisSettings {

    @Value("${duracloud.stage.snapshot:/export/duracloud/staging}")
    private String duracloudSnapshotStage;

    @Value("${duracloud.stage.restore:/export/duracloud/restore}")
    private String duracloudRestoreStage;

    // The intake service only has a direct queue
    private String directQueueBinding;
    private String directQueueName;

    public String getDuracloudSnapshotStage() {
        return duracloudSnapshotStage;
    }

    public void setDuracloudSnapshotStage(final String duracloudSnapshotStage) {
        // TODO: Change to Paths.get(stage)?
        this.duracloudSnapshotStage = duracloudSnapshotStage;
    }

    public String getDirectQueueBinding() {
        if (directQueueBinding == null) {
            directQueueBinding = "duracloud-intake.direct." + getNode();
        }
        return directQueueBinding;
    }

    public String getDirectQueueName() {
        if (directQueueName == null) {
            directQueueName = "duracloud-intake-direct-" + getNode();
        }
        return directQueueName;
    }

    @Override
    public String getInboundKey() {
        return directQueueBinding;
    }

    public String getDuracloudRestoreStage() {
        return duracloudRestoreStage;
    }

    public void setDuracloudRestoreStage(final String duracloudRestoreStage) {
        this.duracloudRestoreStage = duracloudRestoreStage;
    }
}
