package org.chronopolis.intake.duracloud.config;

import org.chronopolis.intake.duracloud.config.props.Chron;
import org.chronopolis.intake.duracloud.config.props.Constraints;
import org.chronopolis.intake.duracloud.config.props.Duracloud;
import org.chronopolis.intake.duracloud.config.props.Smtp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Configuration settings for the bridge intake service
 * <p>
 * todo: supplement from chron-common module where possible
 * <p>
 * Created by shake on 8/1/14.
 */
@ConfigurationProperties
@EnableConfigurationProperties(value = {
        Chron.class, Duracloud.class, Constraints.class, Smtp.class
})
@SuppressWarnings("UnusedDeclaration")
public class IntakeSettings {
    private final Logger log = LoggerFactory.getLogger(IntakeSettings.class);

    private Smtp smtp = new Smtp();
    private Chron chron = new Chron();
    private Duracloud duracloud = new Duracloud();
    private Constraints constraints = new Constraints();

    /**
     * Boolean to configure SNI for https connections
     */
    private Boolean disableSNI = false;

    // should these be encapsulated by a dpn class?
    /**
     * String of the member uuid we are dealing with (deprecated)
     */
    private String memberUUID;

    /**
     * String value representing the server used for dpn replicating nodes
     */
    private String dpnReplicationServer;

    public String getMemberUUID() {
        return memberUUID;
    }

    public IntakeSettings setMemberUUID(String memberUUID) {
        this.memberUUID = memberUUID;
        return this;
    }

    public String getDpnReplicationServer() {
        return dpnReplicationServer;
    }

    public IntakeSettings setDpnReplicationServer(String dpnReplicationServer) {
        this.dpnReplicationServer = dpnReplicationServer;
        return this;
    }

    public Boolean getDisableSNI() {
        return disableSNI;
    }

    public IntakeSettings setDisableSNI(Boolean disableSNI) {
        this.disableSNI = disableSNI;
        return this;
    }

    public Chron getChron() {
        return chron;
    }

    public IntakeSettings setChron(Chron chron) {
        this.chron = chron;
        return this;
    }

    public Duracloud getDuracloud() {
        return duracloud;
    }

    public IntakeSettings setDuracloud(Duracloud duracloud) {
        this.duracloud = duracloud;
        return this;
    }

    public Constraints getConstraints() {
        return constraints;
    }

    public IntakeSettings setConstraints(Constraints constraints) {
        this.constraints = constraints;
        return this;
    }

    public Smtp getSmtp() {
        return smtp;
    }

    public IntakeSettings setSmtp(Smtp smtp) {
        this.smtp = smtp;
        return this;
    }
}
