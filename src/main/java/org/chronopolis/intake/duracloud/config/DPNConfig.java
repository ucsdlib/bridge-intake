package org.chronopolis.intake.duracloud.config;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import okhttp3.OkHttpClient;
import org.chronopolis.earth.OkTokenInterceptor;
import org.chronopolis.earth.api.BalustradeBag;
import org.chronopolis.earth.api.BalustradeMember;
import org.chronopolis.earth.api.BalustradeNode;
import org.chronopolis.earth.api.BalustradeTransfers;
import org.chronopolis.earth.api.Events;
import org.chronopolis.earth.api.LocalAPI;
import org.chronopolis.earth.serializers.ZonedDateTimeDeserializer;
import org.chronopolis.earth.serializers.ZonedDateTimeSerializer;
import org.chronopolis.intake.duracloud.config.inteceptor.HttpTraceInterceptor;
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
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.Optional;
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
    public Optional<String> checkSNI(IntakeSettings settings) throws GeneralSecurityException {
        if (settings.getDisableSNI()) {
            log.info("Disabling SNI");
            System.setProperty("jsse.enableSNIExtension", "false");
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        public void checkClientTrusted(
                                X509Certificate[] certs, String authType) {
                        }

                        public void checkServerTrusted(
                                X509Certificate[] certs, String authType) {
                        }
                    }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        }

        return Optional.of("checked");
    }

    @Bean
    public LocalAPI localAPI(IntakeSettings settings) {
        String endpoint = settings.getDpn().getEndpoint();

        if (!endpoint.endsWith("/")) {
            endpoint = endpoint + "/";
        }

        Gson gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeSerializer())
                .registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeDeserializer())
                .serializeNulls()
                .create();

        OkHttpClient okClient = new OkHttpClient.Builder()
                .addInterceptor(new HttpTraceInterceptor())
                .addInterceptor(new OkTokenInterceptor(settings.getDpn().getApiKey()))
                .readTimeout(5, TimeUnit.HOURS)
                .build();

        Retrofit adapter = new Retrofit.Builder()
                .baseUrl(endpoint)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .client(okClient)
                .build();

        return new LocalAPI().setNode("chron")
                .setEventsAPI(adapter.create(Events.class))
                .setBagAPI(adapter.create(BalustradeBag.class))
                .setNodeAPI(adapter.create(BalustradeNode.class))
                .setMemberAPI(adapter.create(BalustradeMember.class))
                .setTransfersAPI(adapter.create(BalustradeTransfers.class));
    }

}
