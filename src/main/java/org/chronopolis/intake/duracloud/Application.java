package org.chronopolis.intake.duracloud;

import org.chronopolis.common.ace.CredentialRequestInterceptor;
import org.chronopolis.common.mail.MailUtil;
import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.common.settings.IngestAPISettings;
import org.chronopolis.common.settings.SMTPSettings;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.batch.BaggingTasklet;
import org.chronopolis.intake.duracloud.batch.ReplicationTasklet;
import org.chronopolis.intake.duracloud.batch.SnapshotJobManager;
import org.chronopolis.intake.duracloud.batch.SnapshotTasklet;
import org.chronopolis.intake.duracloud.config.IntakeSettings;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.scheduled.Bridge;
import org.chronopolis.rest.api.ErrorLogger;
import org.chronopolis.rest.api.IngestAPI;
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
import retrofit.RestAdapter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

// import org.chronopolis.earth.api.LocalAPI;
// import org.chronopolis.intake.duracloud.remote.model.SnapshotDetails;

/**
 * Quick main class thrown together for doing integration testing of the services
 *
 * Created by shake on 9/28/15.
 */
@SpringBootApplication
@EnableBatchProcessing
@ComponentScan(basePackageClasses = {DPNSettings.class, IntakeSettings.class, Bridge.class})
public class Application implements CommandLineRunner {
    private final Logger log = LoggerFactory.getLogger(Application.class);

    @Autowired
    SnapshotJobManager manager;

    @Autowired
    Bridge bridge;

    public static void main(String[] args) {
        SpringApplication.exit(SpringApplication.run(Application.class));
    }

    @Override
    public void run(String... strings) throws Exception {
        boolean done = false;
        System.out.println("Enter 'q' to quit; 'p' or 'b' to poll the bridge server");
        while (!done) {
            String input = readLine();
            if ("q".equalsIgnoreCase(input)) {
                done = true;
            } else if ("t".equalsIgnoreCase(input)) {
                test();
            } else if ("p".equalsIgnoreCase(input) || "b".equalsIgnoreCase(input)) {
                try {
                    bridge.findSnapshots();
                } catch (Exception e) {
                    log.error("Error calling bridge!", e);
                }
            }
        }

        // SpringApplication.exit(context);
    }

    // Test based on some static content
    private void test() {
        /*
        log.info("Push chron: {} Push DPN: {}", settings.pushChronopolis(), settings.pushDPN());
        SnapshotDetails details = new SnapshotDetails();
        details.setSnapshotId("erik-3-erik-test-space-2014-02-21-20-17-58");
        manager.startSnapshotTasklet(details);
        */
    }

    private String readLine() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            return reader.readLine();
        } catch (IOException ex) {
            throw new RuntimeException("Can't read from STDIN");
        }
    }

    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    IngestAPI ingestAPI(IngestAPISettings settings) {
        String endpoint = settings.getIngestEndpoints().get(0);

        // TODO: This can timeout on long polls, see SO for potential fix
        // http://stackoverflow.com/questions/24669309/how-to-increase-timeout-for-retrofit-requests-in-robospice-android
        RestAdapter adapter = new RestAdapter.Builder()
                .setEndpoint(endpoint)
                .setErrorHandler(logger())
                .setLogLevel(RestAdapter.LogLevel.FULL)
                .setRequestInterceptor(new CredentialRequestInterceptor(
                        settings.getIngestAPIUsername(),
                        settings.getIngestAPIPassword()))
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
    @JobScope
    ReplicationTasklet replicationTasklet(@Value("#{jobParameters[name]}") String name,
                                          @Value("#{jobParameters[depositor]}") String depositor,
                                          @Value("#{jobParameters[receipt]}") String receipt,
                                          IntakeSettings settings,
                                          IngestAPI ingest,
                                          LocalAPI dpn) {
        return new ReplicationTasklet(settings, name, depositor, receipt, ingest, dpn);
    }

    @Bean(destroyMethod = "destroy")
    SnapshotJobManager snapshotJobManager(JobBuilderFactory jobBuilderFactory,
                                          StepBuilderFactory stepBuilderFactory,
                                          JobLauncher jobLauncher,
                                          SnapshotTasklet snapshotTasklet,
                                          BaggingTasklet baggingTasklet,
                                          ReplicationTasklet replicationTasklet,
                                          IntakeSettings settings) {
        return new SnapshotJobManager(jobBuilderFactory,
                stepBuilderFactory,
                jobLauncher,
                snapshotTasklet,
                baggingTasklet,
                replicationTasklet,
                new PropertiesDataCollector(settings));
    }

}
