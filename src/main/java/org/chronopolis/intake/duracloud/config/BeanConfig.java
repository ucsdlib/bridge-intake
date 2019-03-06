package org.chronopolis.intake.duracloud.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.OkHttpClient;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.intake.duracloud.batch.BaggingFactory;
import org.chronopolis.intake.duracloud.batch.ChronFactory;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.batch.check.DepositorCheck;
import org.chronopolis.intake.duracloud.cleaner.Bicarbonate;
import org.chronopolis.intake.duracloud.config.inteceptor.HttpTraceInterceptor;
import org.chronopolis.intake.duracloud.config.props.BagProperties;
import org.chronopolis.intake.duracloud.config.props.Duracloud;
import org.chronopolis.intake.duracloud.config.validator.ChronValidator;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.model.BaggingHistorySerializer;
import org.chronopolis.intake.duracloud.model.HistorySerializer;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.model.ReplicationHistorySerializer;
import org.chronopolis.intake.duracloud.notify.MailNotifier;
import org.chronopolis.intake.duracloud.notify.Notifier;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.rest.api.IngestApiProperties;
import org.chronopolis.rest.api.IngestGenerator;
import org.chronopolis.rest.api.OkBasicInterceptor;
import org.chronopolis.rest.api.ServiceGenerator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.validation.Validator;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Bean declarations
 *
 * @author shake
 */
@Configuration
@EnableConfigurationProperties({BagStagingProperties.class,
        IngestApiProperties.class,
        IntakeSettings.class,
        BagProperties.class})
public class BeanConfig {

    @Bean
    public static Validator configurationPropertiesValidator() {
        return new ChronValidator();
    }

    @Bean
    public Bicarbonate bicarbonate(ServiceGenerator generator,
                                   BagStagingProperties stagingProperties) {
        return new Bicarbonate(generator.depositors(), generator.staging(), stagingProperties);
    }

    @Bean
    public ServiceGenerator generator(IngestApiProperties properties) {
        return new IngestGenerator(properties);
    }

    @Bean
    public Notifier notifier(IntakeSettings settings) {
        return new MailNotifier(settings.getSmtp());
    }

    @Bean
    public BaggingFactory baggingFactory(Notifier notifier,
                                         BagProperties bagProperties,
                                         BagStagingProperties bagStagingProperties) {
        return new BaggingFactory(notifier, bagProperties, bagStagingProperties);
    }

    @Bean
    public ChronFactory chronFactory(ServiceGenerator generator,
                                     Bicarbonate cleaner,
                                     IntakeSettings settings,
                                     BagStagingProperties properties) {
        return new ChronFactory(generator.bags(),
                generator.files(),
                generator.staging(),
                generator.depositors(),
                cleaner,
                settings,
                properties);
    }

    @Bean
    public DepositorCheck depositorCheck(Notifier notifier,
                                         ServiceGenerator generator) {
        return new DepositorCheck(notifier, generator.depositors());
    }

    @Bean(destroyMethod = "destroy")
    public SnapshotJobManager snapshotJobManager(ChronFactory chronFactory,
                                                 BaggingFactory baggingFactory,
                                                 DepositorCheck depositorCheck) {
        return new SnapshotJobManager(chronFactory, baggingFactory, depositorCheck);
    }

    @Bean
    public List<BridgeContext> bridgeContexts(IntakeSettings settings) {
        List<Duracloud.Bridge> bridges = settings.getDuracloud().getBridge();

        return bridges.stream()
                .map(bridge -> new BridgeContext(
                        apiFor(bridge),
                        bridge.getPrefix(),
                        bridge.getManifest(),
                        bridge.getRestores(),
                        bridge.getSnapshots(),
                        bridge.getPush(),
                        bridge.getName()))
                .collect(Collectors.toList());
    }

    @Bean
    @Profile("!develop")
    public boolean createBridgeContextLoggers(List<BridgeContext> contexts) {
        Logging logging = new Logging();
        contexts.forEach(logging::createLogger);
        return true;
    }

    private BridgeAPI apiFor(Duracloud.Bridge bridge) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(History.class, new HistorySerializer())
                .registerTypeAdapter(BaggingHistory.class, new BaggingHistorySerializer())
                .registerTypeAdapter(ReplicationHistory.class, new ReplicationHistorySerializer())
                .disableHtmlEscaping()
                .create();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new HttpTraceInterceptor())
                .addInterceptor(new OkBasicInterceptor(bridge.getUsername(), bridge.getPassword()))
                .readTimeout(2, TimeUnit.MINUTES)
                .build();

        Retrofit adapter = new Retrofit.Builder()
                .baseUrl(bridge.getEndpoint())
                .addConverterFactory(GsonConverterFactory.create(gson))
                .client(client)
                .build();

        return adapter.create(BridgeAPI.class);
    }

}
