package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Some misc properties for configuration
 *
 * @author shake
 */
@Validated
@ConfigurationProperties(prefix = "chron")
public class Chron {

    /**
     * Prefix to use when pushing to chronopolis
     */
    private String prefix = "";

    private String workDirectory = "/tmp/chronopolis";

    public String getPrefix() {
        return prefix;
    }

    public Chron setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public String getWorkDirectory() {
        return workDirectory;
    }

    public Chron setWorkDirectory(String workDirectory) {
        this.workDirectory = workDirectory;
        return this;
    }
}
