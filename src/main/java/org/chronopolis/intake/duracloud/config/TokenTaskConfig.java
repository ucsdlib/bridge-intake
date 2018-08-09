package org.chronopolis.intake.duracloud.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.chronopolis.common.ace.AceConfiguration;
import org.chronopolis.common.concurrent.TrackingThreadPoolExecutor;
import org.chronopolis.intake.duracloud.model.FixityDeserializer;
import org.chronopolis.intake.duracloud.model.ManifestEntryDeserializer;
import org.chronopolis.rest.api.IngestAPIProperties;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.serializers.ZonedDateTimeDeserializer;
import org.chronopolis.rest.models.serializers.ZonedDateTimeSerializer;
import org.chronopolis.rest.models.storage.Fixity;
import org.chronopolis.tokenize.ManifestEntry;
import org.chronopolis.tokenize.batch.ImsServiceWrapper;
import org.chronopolis.tokenize.mq.artemis.ArtemisSupervisor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;

import java.time.ZonedDateTime;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Beans required for Tokenization
 *
 * This starts an ActiveMQ server and all consumers for the server
 *
 * @author shake
 */
@Configuration
@Profile("!disable-tokenizer")
@EnableConfigurationProperties({IngestAPIProperties.class, AceConfiguration.class})
public class TokenTaskConfig {

    @Bean(destroyMethod = "stop")
    public EmbeddedActiveMQ activeMQ() throws Exception {
        return new EmbeddedActiveMQ().start();
    }

    @DependsOn("activeMQ")
    @Bean(destroyMethod = "close")
    public ServerLocator serverLocator() {
        return ActiveMQClient.createServerLocatorWithoutHA(
                new TransportConfiguration(InVMConnectorFactory.class.getName()));
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // could have this as a separate bean but really not that important imo
        SimpleModule module = new SimpleModule();
        module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        module.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        module.addDeserializer(ManifestEntry.class, new ManifestEntryDeserializer());
        module.addDeserializer(Fixity.class, new FixityDeserializer());
        mapper.registerModule(module);
        return mapper;
    }

    @Bean
    public ImsServiceWrapper imsServiceWrapper(AceConfiguration configuration) {
        return new ImsServiceWrapper(configuration.getIms());
    }

    @DependsOn("serverLocator")
    @Bean(destroyMethod = "close")
    public ArtemisSupervisor supervisor(ObjectMapper mapper,
                                        ServerLocator serverLocator,
                                        ServiceGenerator generator,
                                        ImsServiceWrapper imsServiceWrapper) throws Exception {
        return new ArtemisSupervisor(serverLocator, mapper, generator.tokens(), imsServiceWrapper);
    }

    @Bean(destroyMethod = "shutdownNow")
    public TrackingThreadPoolExecutor<Bag> executor() {
        return new TrackingThreadPoolExecutor<>(4, 4, 1, MINUTES, new LinkedBlockingQueue<>());
    }

}
