package org.chronopolis.intake.duracloud.config;

import com.google.common.base.Optional;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.OkHttpClient;
import org.chronopolis.common.ace.OkBasicInterceptor;
import org.chronopolis.common.dpn.OkTokenInterceptor;
import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.intake.duracloud.DateTimeDeserializer;
import org.chronopolis.intake.duracloud.DateTimeSerializer;
import org.chronopolis.intake.duracloud.config.inteceptor.HttpTraceInterceptor;
import org.chronopolis.intake.duracloud.model.BaggingHistory;
import org.chronopolis.intake.duracloud.model.BaggingHistorySerializer;
import org.chronopolis.intake.duracloud.model.HistorySerializer;
import org.chronopolis.intake.duracloud.model.ReplicationHistory;
import org.chronopolis.intake.duracloud.model.ReplicationHistorySerializer;
import org.chronopolis.intake.duracloud.remote.BridgeAPI;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.chronopolis.rest.api.ErrorLogger;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for our beans
 * <p/>
 * Created by shake on 9/30/14.
 */
@Configuration
public class DPNConfig {
    private final Logger log = LoggerFactory.getLogger(DPNConfig.class);

    @Bean
    ErrorLogger logger() {
        return new ErrorLogger();
    }

    @Bean
    Optional<Object> checkSNI(IntakeSettings settings) throws GeneralSecurityException {
        if (settings.getDisableSNI()) {
            System.setProperty("jsse.enableSNIExtension", "false");
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        public void checkClientTrusted(
                                java.security.cert.X509Certificate[] certs, String authType) {
                        }

                        public void checkServerTrusted(
                                java.security.cert.X509Certificate[] certs, String authType) {
                        }
                    }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        }

        return Optional.absent();
    }

    @Bean
    BridgeAPI bridgeAPI(IntakeSettings settings) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(History.class, new HistorySerializer())
                .registerTypeAdapter(BaggingHistory.class, new BaggingHistorySerializer())
                .registerTypeAdapter(ReplicationHistory.class, new ReplicationHistorySerializer())
                .disableHtmlEscaping()
                .create();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(new HttpTraceInterceptor())
                .addInterceptor(new OkBasicInterceptor(
                        settings.getBridgeUsername(),
                        settings.getBridgePassword()))
                .readTimeout(2, TimeUnit.MINUTES)
                .build();

        Retrofit adapter = new Retrofit.Builder()
                .baseUrl(settings.getBridgeEndpoint())
                .addConverterFactory(GsonConverterFactory.create(gson))
                .client(client)
                .build();

        return adapter.create(BridgeAPI.class);
    }

    @Bean
    LocalAPI localAPI(DPNSettings settings) {
        String endpoint = settings.getDPNEndpoints().get(0);

        if (!endpoint.endsWith("/")) {
            endpoint = endpoint + "/";
        }

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(DateTime.class, new DateTimeSerializer())
                .registerTypeAdapter(DateTime.class, new DateTimeDeserializer())
                // .registerTypeAdapter(Replication.Status.class, new ReplicationStatusSerializer())
                // .registerTypeAdapter(Replication.Status.class, new ReplicationStatusDeserializer())
                .serializeNulls()
                .create();

        OkHttpClient okClient = new OkHttpClient.Builder()
                .addInterceptor(new HttpTraceInterceptor())
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
