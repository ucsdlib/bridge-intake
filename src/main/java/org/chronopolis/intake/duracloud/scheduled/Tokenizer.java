package org.chronopolis.intake.duracloud.scheduled;

import com.google.common.collect.ImmutableMap;
import org.chronopolis.common.concurrent.TrackingThreadPoolExecutor;
import org.chronopolis.common.storage.BagStagingProperties;
import org.chronopolis.common.util.Filter;
import org.chronopolis.rest.api.BagService;
import org.chronopolis.rest.api.IngestAPIProperties;
import org.chronopolis.rest.api.ServiceGenerator;
import org.chronopolis.rest.api.TokenService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.chronopolis.tokenize.BagProcessor;
import org.chronopolis.tokenize.batch.ChronopolisTokenRequestBatch;
import org.chronopolis.tokenize.filter.HttpFilter;
import org.chronopolis.tokenize.scheduled.TokenTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageImpl;
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
public class Tokenizer {
    private final Logger log = LoggerFactory.getLogger("tokenizer-log");

    private final BagService bags;
    private final TokenService tokens;
    private final IngestAPIProperties ingestProperties;
    private final BagStagingProperties stagingProperties;
    private final ChronopolisTokenRequestBatch batch;
    private final TrackingThreadPoolExecutor<Bag> executor;


    public Tokenizer(ServiceGenerator generator,
                     IngestAPIProperties ingestProperties,
                     BagStagingProperties stagingProperties,
                     ChronopolisTokenRequestBatch batch,
                     TrackingThreadPoolExecutor<Bag> executor) {
        this.bags = generator.bags();
        this.tokens = generator.tokens();
        this.ingestProperties = ingestProperties;
        this.stagingProperties = stagingProperties;
        this.batch = batch;
        this.executor = executor;
    }

    @Scheduled(cron = "${ingest.cron.tokens:0/30 * * * * *}")
    public void tokenize() {
        log.info("Searching for bags to tokenize");

        // Query ingest API
        Call<PageImpl<Bag>> call = bags.get(
                ImmutableMap.of("status", BagStatus.DEPOSITED,
                        "region_id", stagingProperties.getPosix().getId(),
                        "creator", ingestProperties.getUsername()));
        try {
            Response<PageImpl<Bag>> response = call.execute();
            if (response.isSuccessful()) {
                log.debug("Found {} bags for tokenization", response.body().getSize());
                for (Bag bag : response.body()) {
                    Filter<String> filter = new HttpFilter(bag.getId(), tokens);
                    executor.submitIfAvailable(new BagProcessor(bag, filter, stagingProperties, batch), bag);
                }
            }
        } catch (IOException e) {
            log.error("Error communicating with the ingest server", e);
        }

    }
}
