package org.chronopolis.intake.duracloud;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.batch.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.batch.support.APIHolder;
import org.chronopolis.intake.duracloud.config.DPNConfig;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.notify.MailNotifier;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.scheduled.Bridge;
import org.chronopolis.intake.duracloud.service.ChronService;
import org.chronopolis.rest.api.ErrorLogger;
import org.chronopolis.rest.api.IngestAPIProperties;
import org.chronopolis.rest.api.IngestGenerator;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.tokenize.scheduled.TokenTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;


/**
 * Quick main class thrown together for doing integration testing of the services
 *
 * @author shake
 */
@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties({IntakeSettings.class, BagProperties.class, BagStagingProperties.class})
@ComponentScan(basePackageClasses = {Bridge.class, ChronService.class, DPNConfig.class, TokenTask.class})
public class Application implements CommandLineRunner {
    private final Logger log = LoggerFactory.getLogger(Application.class);

    @Autowired
    ChronService service;

    public static void main(String[] args) {
        SpringApplication.exit(SpringApplication.run(Application.class));
    }

    @Override
    public void run(String... strings) throws Exception {
        service.run();
    }

    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    ServiceGenerator generator(IngestAPIProperties properties) {
        return new IngestGenerator(properties);
    }

    @Bean
    Notifier notifier(IntakeSettings settings) {
        return new MailNotifier(settings.getSmtp());
    }

    @Bean
    @JobScope
    BaggingTasklet baggingTasklet(@Value("#{jobParameters[snapshotId]}") String snapshotId,
                                  @Value("#{jobParameters[depositor]}") String depositor,
                                  IntakeSettings settings,
                                  BagProperties properties,
                                  BagStagingProperties stagingProperties,
                                  BridgeAPI bridge,
                                  Notifier notifier) {
        return new BaggingTasklet(snapshotId, depositor, settings, properties, stagingProperties, bridge, notifier);
    }

    @Bean
    APIHolder holder(ServiceGenerator generator, BridgeAPI bridge, LocalAPI dpn) {
        return new APIHolder(generator, bridge, dpn);
    }

    @Bean(destroyMethod = "destroy")
    SnapshotJobManager snapshotJobManager(JobBuilderFactory jobBuilderFactory,
                                          StepBuilderFactory stepBuilderFactory,
                                          JobLauncher jobLauncher,
                                          APIHolder holder,
                                          BaggingTasklet baggingTasklet,
                                          IntakeSettings settings) {
        return new SnapshotJobManager(jobBuilderFactory,
                stepBuilderFactory,
                jobLauncher,
                holder,
                baggingTasklet,
                new PropertiesDataCollector(settings));
    }

}
