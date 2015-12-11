package org.chronopolis.intake.duracloud.test;

import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 *
 * Created by shake on 12/4/15.
 */
@SpringBootApplication
@EnableBatchProcessing
@ComponentScan(basePackageClasses = {DPNSettings.class, IntakeSettings.class})
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.exit(SpringApplication.run(TestApplication.class));
    }

}
