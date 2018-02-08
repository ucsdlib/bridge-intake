package org.chronopolis.intake.duracloud.config;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.PropertiesDataCollector;
import org.chronopolis.intake.duracloud.batch.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.batch.support.APIHolder;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.notify.MailNotifier;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.ErrorLogger;
import org.chronopolis.rest.api.IngestAPIProperties;
import org.chronopolis.rest.api.IngestGenerator;
import org.chronopolis.rest.api.ServiceGenerator;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Test
 *
 * @author shake
 */
@Configuration
@EnableConfigurationProperties({BagStagingProperties.class, IntakeSettings.class, BagProperties.class})
public class BeanConfig {

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
