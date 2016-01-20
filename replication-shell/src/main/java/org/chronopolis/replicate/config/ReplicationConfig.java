package org.chronopolis.replicate.config;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.common.ace.AceService;
import org.chronopolis.common.ace.CredentialRequestInterceptor;
import org.chronopolis.common.ace.OkBasicInterceptor;
import org.chronopolis.common.mail.MailUtil;
import org.chronopolis.common.settings.AceSettings;
import org.chronopolis.common.settings.IngestAPISettings;
import org.chronopolis.common.settings.SMTPSettings;
import org.chronopolis.common.util.URIUtil;
import org.chronopolis.messaging.base.ChronMessage;
import org.chronopolis.messaging.factory.MessageFactory;
import org.chronopolis.replicate.batch.ReplicationJobStarter;
import org.chronopolis.replicate.batch.ReplicationStepListener;
import org.chronopolis.rest.api.ErrorLogger;
import org.chronopolis.rest.api.IngestAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

/**
 * Configuration for the beans used by the replication-shell
 *
 * Created by shake on 4/16/14.
 */
@Configuration
@EnableBatchProcessing
public class ReplicationConfig {
    public final Logger log = LoggerFactory.getLogger(ReplicationConfig.class);

    @Autowired
    JobRepository jobRepository;

    @Value("${debug.retrofit:NONE}")
    String retrofitLogLevel;

    /**
     * Logger to capture why errors happened in Retrofit
     *
     * @return
     */
    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    /**
     * Retrofit adapter for interacting with the ACE REST API
     *
     * @param aceSettings - Settings to connect to ACE with
     * @return
     */
    @Bean
    AceService aceService(AceSettings aceSettings) {
        // Next build the retrofit adapter
        String endpoint = URIUtil.buildAceUri(aceSettings.getAmHost(),
                aceSettings.getAmPort(),
                aceSettings.getAmPath()).toString();

        // TODO: Test
        HttpUrl url = new HttpUrl.Builder()
                .scheme("https")
                .host(aceSettings.getAmHost())
                .port(aceSettings.getAmPort())
                .addPathSegment(aceSettings.getAmPath()).build();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new OkBasicInterceptor(aceSettings.getAmUser(), aceSettings.getAmPassword()))
                .build();

        Retrofit restAdapter = new Retrofit.Builder()
                .baseUrl(endpoint)
                .client(client)
                // .setErrorHandler(logger())
                // .setLogLevel(Retrofit.LogLevel.valueOf(retrofitLogLevel))
                .build();

        return restAdapter.create(AceService.class);
    }

    /**
     * Retrofit adapter for interacting with the ingest-server REST API
     *
     * @param apiSettings - Settings to connect to the ingest-server with
     * @return
     */
    @Bean
    IngestAPI ingestAPI(IngestAPISettings apiSettings) {
        // TODO: Create a list of endpoints
        String endpoint = apiSettings.getIngestEndpoints().get(0);
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new OkBasicInterceptor(apiSettings.getIngestAPIUsername(),
                        apiSettings.getIngestAPIPassword()))
                .build();


        Retrofit adapter = new Retrofit.Builder()
                .baseUrl(endpoint)
                .client(client)
                .addConverterFactory(GsonConverterFactory.create())
                // .setErrorHandler(logger())
                // .setLogLevel(Retrofit.LogLevel.valueOf(retrofitLogLevel))
                // .setLogLevel(Retrofit.LogLevel.FULL)
                .build();

        return adapter.create(IngestAPI.class);
    }

    /**
     * Null producer for the {@link ReplicationJobStarter}
     *
     * @return
     */
    @Bean
    ChronProducer producer() {
        // Return a null producer
        return new ChronProducer() {
            @Override
            public void send(ChronMessage message, String routingKey) {
            }
        };
    }

    /**
     * MessageFactory needed for the {@link ReplicationJobStarter}
     *
     * @param chronopolisSettings
     * @return
     */
    @Bean
    MessageFactory messageFactory(ReplicationSettings chronopolisSettings) {
        return new MessageFactory(chronopolisSettings);
    }

    /**
     * Class to handle creation of replication jobs through spring-batch
     *
     * @param producer
     * @param messageFactory
     * @param settings
     * @param mailUtil
     * @param aceService
     * @param ingestAPI
     * @param replicationStepListener
     * @param jobLauncher
     * @param jobBuilderFactory
     * @param stepBuilderFactory
     * @return
     */
    @Bean
    ReplicationJobStarter jobStarter(ChronProducer producer,
                                     MessageFactory messageFactory,
                                     ReplicationSettings settings,
                                     MailUtil mailUtil,
                                     AceService aceService,
                                     IngestAPI ingestAPI,
                                     ReplicationStepListener replicationStepListener,
                                     JobLauncher jobLauncher,
                                     JobBuilderFactory jobBuilderFactory,
                                     StepBuilderFactory stepBuilderFactory) {
        return new ReplicationJobStarter(producer,
                messageFactory,
                settings,
                mailUtil,
                aceService,
                ingestAPI,
                replicationStepListener,
                jobLauncher,
                jobBuilderFactory,
                stepBuilderFactory);
    }

    /**
     * Step listener for the {@link ReplicationJobStarter}
     *
     * @param replicationSettings
     * @param mailUtil
     * @return
     */
    @Bean
    ReplicationStepListener replicationStepListener(ReplicationSettings replicationSettings,
                                                    MailUtil mailUtil) {
        return new ReplicationStepListener(replicationSettings, mailUtil);
    }

    /**
     * Class to send email notifications regarding replications
     *
     * @param smtpSettings - The settings to use for smtp messages
     * @return
     */
    @Bean
    MailUtil mailUtil(SMTPSettings smtpSettings) {
        MailUtil mailUtil = new MailUtil();
        mailUtil.setSmtpFrom(smtpSettings.getFrom());
        mailUtil.setSmtpTo(smtpSettings.getTo());
        mailUtil.setSmtpHost(smtpSettings.getHost());
        mailUtil.setSmtpSend(smtpSettings.getSend());
        return mailUtil;
    }

    /**
     * Async task executor so we can have multiple threads execute at once
     * for spring-batch
     *
     * @return
     */
    @Bean
    TaskExecutor simpleAsyncTaskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(2);
        return taskExecutor;
    }


    /**
     * SimpleJobLauncher so that we use the {@link TaskExecutor}
     *
     * @param jobRepository
     * @param simpleAsyncTaskExecutor
     * @return
     */
    @Bean
    JobLauncher jobLauncher(JobRepository jobRepository, TaskExecutor simpleAsyncTaskExecutor) {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(simpleAsyncTaskExecutor);
        return jobLauncher;
    }

}
