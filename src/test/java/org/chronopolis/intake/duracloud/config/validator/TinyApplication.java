package org.chronopolis.intake.duracloud.config.validator;

import org.chronopolis.intake.duracloud.config.props.Chron;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.validation.Validator;

/**
 * Just for {@link ChronValidatorTest}
 *
 * @author shake
 */
@SpringBootApplication
@EnableConfigurationProperties(Chron.class)
public class TinyApplication implements CommandLineRunner  {

    public TinyApplication() {
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder(TinyApplication.class)
                .logStartupInfo(false)
                .bannerMode(Banner.Mode.OFF)
                .run(args);
    }


    @Override
    public void run(String... args) {
    }

    @Bean
    public static Validator configurationPropertiesValidator() {
        return new ChronValidator();
    }
}
