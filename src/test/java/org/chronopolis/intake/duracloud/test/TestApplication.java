package org.chronopolis.intake.duracloud.test;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 *
 * Created by shake on 12/4/15.
 */
@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties({IntakeSettings.class, BagProperties.class, BagStagingProperties.class})
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.exit(SpringApplication.run(TestApplication.class));
    }

}
