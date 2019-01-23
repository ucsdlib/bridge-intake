package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.chronopolis.common.concurrent.TrackingThreadPoolExecutor;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.IngestApiProperties;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.rest.api.TokenService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.enums.BagStatus;
import org.chronopolis.rest.models.page.SpringPage;
import org.chronopolis.tokenize.BagProcessor;
import org.chronopolis.tokenize.filter.ProcessingFilter;
import org.chronopolis.tokenize.scheduled.TokenTask;
import org.chronopolis.tokenize.supervisor.TokenWorkSupervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

/**
 * Replacement for {@link TokenTask} because I messed up
 *
 * @author shake
 */
@Component
@EnableScheduling
@Profile("!disable-tokenizer")
public class Tokenizer {
    private final Logger log = LoggerFactory.getLogger(Tokenizer.class);

    private final BagService bags;
    private final TokenService tokens;
    private final TokenWorkSupervisor supervisor;
    private final IngestApiProperties ingestProperties;
    private final BagStagingProperties stagingProperties;
    private final TrackingThreadPoolExecutor<Bag> executor;

    public Tokenizer(ServiceGenerator generator,
                     TokenWorkSupervisor supervisor,
                     IngestApiProperties ingestProperties,
                     BagStagingProperties stagingProperties,
                     TrackingThreadPoolExecutor<Bag> executor) {
        this.bags = generator.bags();
        this.tokens = generator.tokens();
        this.supervisor = supervisor;
        this.ingestProperties = ingestProperties;
        this.stagingProperties = stagingProperties;
        this.executor = executor;
    }

    @Scheduled(cron = "${ingest.cron.tokens:0 0/10 * * * *}")
    public void tokenize() {
        log.info("Searching for bags to tokenize");

        // Query ingest API
        Call<SpringPage<Bag>> call = bags.get(
                ImmutableMap.of("status", BagStatus.INITIALIZED.toString(),
                        "region", stagingProperties.getPosix().getId().toString(),
                        "creator", ingestProperties.getUsername()));

        ProcessingFilter processingFilter = new ProcessingFilter(supervisor);
        try {
            Response<SpringPage<Bag>> response = call.execute();
            if (response.isSuccessful()) {
                // execute consumers here?

                log.debug("Found {} bags for tokenization", response.body().getNumberOfElements());
                for (Bag bag : response.body()) {
                    // todo: get a proper staging call in the api
                    Call<Bag> stagingCall = bags.get(bag.getId());
                    bag = stagingCall.execute().body();
                    if (bag.getBagStorage() != null) {

                        // HttpFilter httpFilter = new HttpFilter(bag.getId(), tokens);
                        BagProcessor processor = new BagProcessor(bag,
                                ImmutableSet.of(processingFilter), //, httpFilter),
                                stagingProperties,
                                supervisor);
                        executor.submitIfAvailable(processor, bag);
                    } else {
                        log.info("[{}] Waiting for storage init", bag.getName());
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error communicating with the ingest server", e);
        }

    }
}
