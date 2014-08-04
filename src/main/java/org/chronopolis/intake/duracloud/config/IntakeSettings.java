package org.chronopolis.intake.duracloud.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by shake on 8/1/14.
 */
@Component
@SuppressWarnings("UnusedDeclaration")
public class IntakeSettings {

    @Value("${intake.duracloudStage:/export/duracloud/staging}")
    private String duracloudStage;

    public String getDuracloudStage() {
        return duracloudStage;
    }

    public void setDuracloudStage(final String duracloudStage) {
        // TODO: Change to Paths.get(stage)?
        this.duracloudStage = duracloudStage;
    }
}
