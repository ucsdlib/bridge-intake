package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * @author shake
 */
@ConfigurationProperties(prefix = "chron")
public class Chron {

    /**
     * Prefix to use when pushing to chronopolis
     */
    private String prefix = "";

    public String getPrefix() {
        return prefix;
    }

    public Chron setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }
}
