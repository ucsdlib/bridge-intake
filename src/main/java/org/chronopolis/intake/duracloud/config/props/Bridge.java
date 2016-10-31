package org.chronopolis.intake.duracloud.config.props;

/**
 * Created by shake on 10/31/16.
 */
public class Bridge {

    private String user;
    private String password;

    public String getUser() {
        return user;
    }

    public Bridge setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Bridge setPassword(String password) {
        this.password = password;
        return this;
    }
}
