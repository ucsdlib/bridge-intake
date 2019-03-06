package org.chronopolis.intake.duracloud.model;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Phaser;

/**
 * Brought over from old dpn code
 *
 * @since 3.0.0
 * @author shake
 */
public class SimpleCallback<E> implements Callback<E> {
    private final Logger log = LoggerFactory.getLogger(SimpleCallback.class);

    private E response;

    /**
     * Same note as before:
     * We start with 2 parties (the response and the client request)
     * When we receive the initial response we deregister so that we
     * can continue to make calls to getResponse
     */
    private Phaser phaser = new Phaser(2);


    @Override
    public void onResponse(@NotNull Call<E> call, @NotNull Response<E> response) {
        if (response.isSuccessful()) {
            log.debug("Successfully completed HTTP call with response: {} {}",
                    response.code(),
                    response.message());
            this.response = response.body();
        } else {
            String errorBody;
            try {
                errorBody = response.errorBody().string();
            } catch (IOException e) {
                errorBody = e.getMessage();
                log.warn("Error writing response", e);
            }
            log.warn("HTTP call was not successful: {} {} {}",
                    response.raw().request().url(),
                    response.code(),
                    errorBody);
        }

        phaser.arriveAndDeregister();
    }

    @Override
    public void onFailure(@NotNull Call<E> call, @NotNull Throwable throwable) {
        log.warn("Error in http call", throwable);
        phaser.arriveAndDeregister();
    }

    public Optional<E> getResponse() {
        phaser.arriveAndAwaitAdvance();
        return Optional.ofNullable(response);
    }
}
