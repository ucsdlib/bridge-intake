package org.chronopolis.intake.duracloud.config;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.OkHttpClient;
import org.chronopolis.common.ace.OkBasicInterceptor;
import org.chronopolis.common.dpn.TokenInterceptor;
import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.earth.OkTokenInterceptor;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.models.Replication;
import org.chronopolis.earth.serializers.ReplicationStatusDeserializer;
import org.chronopolis.earth.serializers.ReplicationStatusSerializer;
import org.chronopolis.intake.duracloud.DateTimeDeserializer;
import org.chronopolis.intake.duracloud.DateTimeSerializer;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.model.BaggingHistorySerializer;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.model.ReplicationHistorySerializer;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.rest.api.ErrorLogger;
import org.joda.time.DateTime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit2.GsonConverterFactory;
import retrofit2.Retrofit;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for our beans
 *
 * Created by shake on 9/30/14.
 */
@Configuration
public class DPNConfig {

    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    BridgeAPI bridgeAPI(IntakeSettings settings) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(BaggingHistory.class, new BaggingHistorySerializer())
                .registerTypeAdapter(ReplicationHistory.class, new ReplicationHistorySerializer())
                .create();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new OkBasicInterceptor(
                        settings.getBridgeUsername(),
                        settings.getBridgePassword()))
                .build();

        Retrofit adapter = new Retrofit.Builder()
                //.setRequestInterceptor(new CredentialRequestInterceptor(
                //        settings.getBridgeUsername(),
                //        settings.getBridgePassword()))
                .baseUrl(settings.getBridgeEndpoint())
                .addConverterFactory(GsonConverterFactory.create(gson))
                .client(client)
                // .setLogLevel(RestAdapter.LogLevel.FULL)
                .build();

        return adapter.create(BridgeAPI.class);
    }

    @Bean
    LocalAPI localAPI(DPNSettings settings) {
        String endpoint = settings.getDPNEndpoints().get(0);
        TokenInterceptor interceptor = new TokenInterceptor(settings.getApiKey());

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(DateTime.class, new DateTimeSerializer())
                .registerTypeAdapter(DateTime.class, new DateTimeDeserializer())
                .registerTypeAdapter(Replication.Status.class, new ReplicationStatusSerializer())
                .registerTypeAdapter(Replication.Status.class, new ReplicationStatusDeserializer())
                .serializeNulls()
                .create();

        OkHttpClient okClient = new OkHttpClient.Builder()
                .addInterceptor(new OkTokenInterceptor(settings.getApiKey()))
                .readTimeout(5, TimeUnit.HOURS)
                .build();

        Retrofit adapter = new Retrofit.Builder()
                .baseUrl(endpoint)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .client(okClient)
                .build();

        return new LocalAPI().setNode("chron")
                .setBagAPI(adapter.create(BalustradeBag.class))
                .setNodeAPI(adapter.create(BalustradeNode.class))
                .setTransfersAPI(adapter.create(BalustradeTransfers.class));
    }

}
