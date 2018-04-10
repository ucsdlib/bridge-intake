package org.chronopolis.intake.duracloud.config;

import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.PropertiesDataCollector;
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
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Config for our beans
 *
 * @author shake
 */
@Configuration
@EnableConfigurationProperties({BagStagingProperties.class,
        IntakeSettings.class,
        BagProperties.class})
public class BeanConfig {

    @Bean
    public Bicarbonate bicarbonate(ServiceGenerator generator,
                                   BagStagingProperties stagingProperties) {
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
    public APIHolder holder(ServiceGenerator generator, BridgeAPI bridge, LocalAPI dpn) {
        return new APIHolder(generator, bridge, dpn);
    }

    @Bean(destroyMethod = "destroy")
    public SnapshotJobManager snapshotJobManager(Notifier notifier,
                                                 BagProperties bagProperties,
                                                 BagStagingProperties bagStagingProperties,
                                                 Bicarbonate cleaningManager,
                                                 APIHolder holder,
                                                 IntakeSettings settings) {
        return new SnapshotJobManager(notifier,
                cleaningManager,
                bagProperties,
                settings,
                bagStagingProperties,
                holder,
                new PropertiesDataCollector(settings));
    }

}
