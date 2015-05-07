package org.chronopolis.intake.duracloud.config;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.chronopolis.common.ace.CredentialRequestInterceptor;
import org.chronopolis.common.dpn.DPNService;
import org.chronopolis.common.dpn.TokenInterceptor;
import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.intake.duracloud.DateTimeDeserializer;
import org.chronopolis.intake.duracloud.DateTimeSerializer;
import org.chronopolis.rest.api.ErrorLogger;
import org.joda.time.DateTime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit.RestAdapter;
import retrofit.converter.GsonConverter;

/**
 * Created by shake on 9/30/14.
 */
@Configuration
public class DPNConfig {

    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    DPNService dpnService(DPNSettings dpnSettings) {
        String endpoint = dpnSettings.getDPNEndpoints().get(0);

        TokenInterceptor interceptor = new TokenInterceptor(dpnSettings.getApiKey());

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(DateTime.class, new DateTimeSerializer())
                .registerTypeAdapter(DateTime.class, new DateTimeDeserializer())
                .create();

        RestAdapter restAdapter = new RestAdapter.Builder()
                .setEndpoint(endpoint)
                .setRequestInterceptor(interceptor)
                .setErrorHandler(logger())
                .setLogLevel(RestAdapter.LogLevel.FULL)
                .setConverter(new GsonConverter(gson))
                .build();

        return restAdapter.create(DPNService.class);
    }

}
