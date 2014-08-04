package org.chronopolis.intake.duracloud.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by shake on 8/4/14.
 */
@Component
@SuppressWarnings("UnusedDeclaration")
public class JPASettings {

    @Value("${db.driver:org.h2.Driver}")
    private String dbDriver;

    @Value("${db.url:jdbc:h2:intakedb}")
    private String dbURL;

    @Value("${db.user:h2}")
    private String dbUser;

    @Value("${db.password:h2}")
    private String dbPassword;


    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(final String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(final String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbURL() {
        return dbURL;
    }

    public void setDbURL(final String dbURL) {
        this.dbURL = dbURL;
    }

    public String getDbDriver() {
        return dbDriver;
    }

    public void setDbDriver(final String dbDriver) {
        this.dbDriver = dbDriver;
    }
}
