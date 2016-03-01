package org.chronopolis.intake.duracloud.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 *
 * Created by shake on 3/1/16.
 */
@Component
@Profile("default")
public class ProductionService implements ChronService {
    private final Logger log = LoggerFactory.getLogger(ProductionService.class);

    @Autowired
    ApplicationContext context;

    @Override
    public void run() {
        System.out.close();
        System.err.close();

        while(true) {
            try {
                TimeUnit.SECONDS.sleep(300);
                log.trace("sleep low, sweet chariot");
            } catch (InterruptedException e) {
                log.info("Shutting down");
                SpringApplication.exit(context);
            }
        }

    }
}
