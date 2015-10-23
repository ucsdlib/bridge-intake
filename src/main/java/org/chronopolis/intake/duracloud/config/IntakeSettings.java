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

    @Value("${duracloud.manifest:manifest-sha256.txt}")
    private String duracloudManifest;

    @Value("${duracloud.host:test.duracloud.org}")
    private String duracloudHost;

    @Value("${duracloud.bridge.endpoint:http://localhost:8080/bridge}")
    private String bridgeEndpoint;

    @Value("${duracloud.bridge.username:root}")
    private String bridgeUsername;

    @Value("${duracloud.bridge.password:password}")
    private String bridgePassword;

    @Value("${push.chronopolis:true}")
    private Boolean pushChronopolis;

    @Value("${push.dpn:true}")
    private Boolean pushDPN;

    public String getDuracloudSnapshotStage() {
        return duracloudSnapshotStage;
    }

    public void setDuracloudSnapshotStage(final String duracloudSnapshotStage) {
        // TODO: Change to Paths.get(stage)?
        this.duracloudSnapshotStage = duracloudSnapshotStage;
    }

    public String getDuracloudRestoreStage() {
        return duracloudRestoreStage;
    }

    public void setDuracloudRestoreStage(final String duracloudRestoreStage) {
        this.duracloudRestoreStage = duracloudRestoreStage;
    }

    public String getDuracloudManifest() {
        return duracloudManifest;
    }

    public void setDuracloudManifest(final String duracloudManifest) {
        this.duracloudManifest = duracloudManifest;
    }

    public String getBridgeEndpoint() {
        return bridgeEndpoint;
    }

    public IntakeSettings setBridgeEndpoint(String bridgeEndpoint) {
        this.bridgeEndpoint = bridgeEndpoint;
        return this;
    }

    public Boolean pushChronopolis() {
        return pushChronopolis;
    }

    public IntakeSettings setPushChronopolis(Boolean pushChronopolis) {
        this.pushChronopolis = pushChronopolis;
        return this;
    }

    public Boolean pushDPN() {
        return pushDPN;
    }

    public IntakeSettings setPushDPN(Boolean pushDPN) {
        this.pushDPN = pushDPN;
        return this;
    }

    public String getBridgeUsername() {
        return bridgeUsername;
    }

    public IntakeSettings setBridgeUsername(String bridgeUsername) {
        this.bridgeUsername = bridgeUsername;
        return this;
    }

    public String getBridgePassword() {
        return bridgePassword;
    }

    public IntakeSettings setBridgePassword(String bridgePassword) {
        this.bridgePassword = bridgePassword;
        return this;
    }

    public String getDuracloudHost() {
        return duracloudHost;
    }

    public IntakeSettings setDuracloudHost(String duracloudHost) {
        this.duracloudHost = duracloudHost;
        return this;
    }
}
