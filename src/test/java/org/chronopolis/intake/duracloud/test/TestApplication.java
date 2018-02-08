package org.chronopolis.intake.duracloud.test;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.rest.api.IngestAPIProperties;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 *
 * Created by shake on 12/4/15.
 */
@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties({IntakeSettings.class, BagProperties.class, BagStagingProperties.class, IngestAPIProperties.class})
public class TestApplication {

    private static final String TEST_PROFILE = "test";

    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", TEST_PROFILE);
        System.setProperty("logging.file", "bridge-intake.log");
        // new SpringApplicationBuilder().profiles(TEST_PROFILE).main(TestApplication.class).run();
        SpringApplication.exit(new SpringApplicationBuilder(TestApplication.class)
                .run());
    }

}
