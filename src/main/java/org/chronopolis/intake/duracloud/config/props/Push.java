package org.chronopolis.intake.duracloud.config.props;

/**
 * Push enum defining where a snapshot should be pushed to
 *
 * @author shake
 */
public enum Push {
    CHRONOPOLIS(""), @Deprecated DPN(".tar"), NONE("");

    private final String postfix;

    Push(String postfix) {
        this.postfix = postfix;
    }

    public String getPostfix() {
        return postfix;
    }
}
