package org.chronopolis.intake.duracloud.config.props;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 *
 * Created by shake on 3/1/17.
 */
@ConfigurationProperties(prefix = "smtp")
public class Smtp {

    /**
     * Address to use when sending smtp messages
     */
    private String from = "chron-mail@umiacs.umd.edu";

    /*
     * Address to send notification mail to
     */
    private String to = "chronopolis-support-l@ucsd.edu";

    public String getFrom() {
        return from;
    }

    public Smtp setFrom(String from) {
        this.from = from;
        return this;
    }

    public String getTo() {
        return to;
    }

    public Smtp setTo(String to) {
        this.to = to;
        return this;
    }
}
