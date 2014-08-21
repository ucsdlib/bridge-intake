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

    @Value("${intake.stage.duracloud:/export/duracloud/staging}")
    private String duracloudStage;

    // The intake service only has a direct queue
    private String directQueueBinding;
    private String directQueueName;

    public String getDuracloudStage() {
        return duracloudStage;
    }

    public void setDuracloudStage(final String duracloudStage) {
        // TODO: Change to Paths.get(stage)?
        this.duracloudStage = duracloudStage;
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

}
