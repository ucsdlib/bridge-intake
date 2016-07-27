package org.chronopolis.intake.duracloud;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import okhttp3.OkHttpClient;
import org.chronopolis.common.ace.OkBasicInterceptor;
import org.chronopolis.common.mail.MailUtil;
import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.common.settings.IngestAPISettings;
import org.chronopolis.common.settings.SMTPSettings;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.batch.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.batch.SnapshotTasklet;
import org.chronopolis.intake.duracloud.batch.support.APIHolder;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.scheduled.Bridge;
import org.chronopolis.intake.duracloud.service.ChronService;
import org.chronopolis.rest.api.ErrorLogger;
import org.chronopolis.rest.api.IngestAPI;
import org.chronopolis.rest.entities.Bag;
import org.chronopolis.rest.support.PageDeserializer;
import org.chronopolis.rest.support.ZonedDateTimeDeserializer;
import org.chronopolis.rest.support.ZonedDateTimeSerializer;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.domain.PageImpl;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import java.lang.reflect.Type;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

// import org.chronopolis.earth.api.LocalAPI;
// import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;

/**
 * Quick main class thrown together for doing integration testing of the services
 *
 * Created by shake on 9/28/15.
 */
@SpringBootApplication
@EnableBatchProcessing
@ComponentScan(basePackageClasses = {DPNSettings.class, IntakeSettings.class, Bridge.class, ChronService.class})
public class Application implements CommandLineRunner {
    private final Logger log = LoggerFactory.getLogger(Application.class);

    @Autowired
    SnapshotJobManager manager;

    @Autowired
    ChronService service;

    public static void main(String[] args) {
        SpringApplication.exit(SpringApplication.run(Application.class));
    }

    @Override
    public void run(String... strings) throws Exception {
        service.run();
        // SpringApplication.exit(context);
    }

    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    IngestAPI ingestAPI(IngestAPISettings settings) {
        String endpoint = settings.getIngestEndpoints().get(0);

        Type bagPage = new TypeToken<PageImpl<Bag>>() {}.getType();
        Type bagList = new TypeToken<List<Bag>>() {}.getType();
        // Type replPage = new TypeToken<PageImpl<Replication>>() {}.getType();
        //Type replList = new TypeToken<List<Replication>>() {}.getType();

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(bagPage, new PageDeserializer(bagList))
                // .registerTypeAdapter(replPage, new PageDeserializer(replList))
                .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeSerializer())
                .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeDeserializer())
                .create();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new OkBasicInterceptor(
                        settings.getIngestAPIUsername(),
                        settings.getIngestAPIPassword()))
                .readTimeout(5, TimeUnit.HOURS)
                .build();

        Retrofit adapter = new Retrofit.Builder()
                .addConverterFactory(GsonConverterFactory.create(gson))
                .baseUrl(endpoint)
                .client(client)
                .build();

        return adapter.create(IngestAPI.class);

    }

    @Bean
    MailUtil mailUtil(SMTPSettings smtpSettings) {
        MailUtil mailUtil = new MailUtil();
        mailUtil.setSmtpFrom(smtpSettings.getFrom());
        mailUtil.setSmtpTo("shake@umiacs.umd.edu");
        mailUtil.setSmtpHost(smtpSettings.getHost());
        mailUtil.setSmtpSend(smtpSettings.getSend());
        return mailUtil;
    }



    @Bean
    @JobScope
    SnapshotTasklet snapshotTasklet(@Value("#{jobParameters[snapshotId]}") String snapshotID,
                                    @Value("#{jobParameters[depositor]}") String depositor,
                                    @Value("#{jobParameters[collectionName]}") String collectionName,
                                    IntakeSettings settings,
                                    IngestAPI ingestAPI,
                                    LocalAPI localAPI) {
        return new SnapshotTasklet(snapshotID,
                collectionName,
                depositor,
                settings,
                ingestAPI,
                localAPI);
    }

    @Bean
    @JobScope
    BaggingTasklet baggingTasklet(@Value("#{jobParameters[snapshotId]}") String snapshotId,
                                  @Value("#{jobParameters[depositor]}") String depositor,
                                  @Value("#{jobParameters[collectionName]}") String collectionName,
                                  IntakeSettings settings,
                                  BridgeAPI bridge) {
        return new BaggingTasklet(snapshotId, collectionName, depositor, settings, bridge);
    }

    @Bean
    APIHolder holder(IngestAPI ingest, BridgeAPI bridge, LocalAPI dpn) {
        return new APIHolder(ingest, bridge, dpn);
    }

    @Bean(destroyMethod = "destroy")
    SnapshotJobManager snapshotJobManager(JobBuilderFactory jobBuilderFactory,
                                          StepBuilderFactory stepBuilderFactory,
                                          JobLauncher jobLauncher,
                                          APIHolder holder,
                                          SnapshotTasklet snapshotTasklet,
                                          BaggingTasklet baggingTasklet,
                                          IntakeSettings settings) {
        return new SnapshotJobManager(jobBuilderFactory,
                stepBuilderFactory,
                jobLauncher,
                holder,
                snapshotTasklet,
                baggingTasklet,
                new PropertiesDataCollector(settings));
    }

}
