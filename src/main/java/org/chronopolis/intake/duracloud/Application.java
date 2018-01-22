package org.chronopolis.intake.duracloud;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.batch.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.batch.support.APIHolder;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
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
import org.springframework.boot.system.ApplicationPidFileWriter;
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
@ComponentScan(basePackageClasses = {Bridge.class, ChronService.class, DPNConfig.class})
public class Application implements CommandLineRunner {

    private final ChronService service;

    @Autowired
    public Application(ChronService service) {
        this.service = service;
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(Application.class);
        application.addListeners(new ApplicationPidFileWriter());
        SpringApplication.exit(application.run(args));
    }

    @Override
    public void run(String... strings) {
        service.run();
    }

    @Bean
    public Bicarbonate bicarbonate(ServiceGenerator generator, BagStagingProperties stagingProperties) {
        return new Bicarbonate(generator, stagingProperties);
    }

    @Bean
    public ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    public ServiceGenerator generator(IngestAPIProperties properties) {
        return new IngestGenerator(properties);
    }

    @Bean
    public Notifier notifier(IntakeSettings settings) {
        return new MailNotifier(settings.getSmtp());
    }

    @Bean
    @JobScope
    public BaggingTasklet baggingTasklet(@Value("#{jobParameters[snapshotId]}") String snapshotId,
                                  @Value("#{jobParameters[depositor]}") String depositor,
                                  IntakeSettings settings,
                                  BagProperties properties,
                                  BagStagingProperties stagingProperties,
                                  BridgeAPI bridge,
                                  Notifier notifier) {
        return new BaggingTasklet(snapshotId, depositor, settings, properties, stagingProperties, bridge, notifier);
    }

    @Bean
    public APIHolder holder(ServiceGenerator generator, BridgeAPI bridge, LocalAPI dpn) {
        return new APIHolder(generator, bridge, dpn);
    }

    @Bean(destroyMethod = "destroy")
    public SnapshotJobManager snapshotJobManager(Bicarbonate cleaningManager,
                                          JobBuilderFactory jobBuilderFactory,
                                          StepBuilderFactory stepBuilderFactory,
                                          JobLauncher jobLauncher,
                                          APIHolder holder,
                                          BaggingTasklet baggingTasklet,
                                          IntakeSettings settings) {
        return new SnapshotJobManager(cleaningManager,
                jobBuilderFactory,
                stepBuilderFactory,
                jobLauncher,
                holder,
                baggingTasklet,
                new PropertiesDataCollector(settings));
    }

}
