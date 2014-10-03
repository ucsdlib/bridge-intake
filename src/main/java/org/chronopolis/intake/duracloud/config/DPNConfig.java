package org.chronopolis.intake.duracloud.config;

import org.chronopolis.common.dpn.DPNService;
import org.chronopolis.common.settings.DPNSettings;
import org.chronopolis.common.util.URIUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit.RestAdapter;

/**
 * Created by shake on 9/30/14.
 */
@Configuration
public class DPNConfig {

    @Bean
    DPNService dpnService(DPNSettings dpnSettings) {
        String endpoint = URIUtil.buildAceUri(dpnSettings.getDpnWebHost(),
                dpnSettings.getDpnWebPort(),
                dpnSettings.getDpnWebPath()).toString();


        /*
        CredentialRequestInterceptor interceptor = new CredentialRequestInterceptor(
                dpnSettings.getUser(),
                dpnSettings.getPassword());
                */

        RestAdapter restAdapter = new RestAdapter.Builder()
                .setEndpoint(endpoint)
                .build();

        return restAdapter.create(DPNService.class);
    }

}
