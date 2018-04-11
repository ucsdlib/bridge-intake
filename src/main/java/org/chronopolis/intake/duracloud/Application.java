package org.chronopolis.intake.duracloud;

import org.chronopolis.intake.duracloud.config.DPNConfig;
import org.chronopolis.intake.duracloud.scheduled.Bridge;
import org.chronopolis.intake.duracloud.service.ChronService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.system.ApplicationPidFileWriter;
import org.springframework.context.annotation.ComponentScan;


/**
 * Quick main class thrown together for doing integration testing of the services
 *
 * @author shake
 */
@SpringBootApplication
@ComponentScan(basePackageClasses = {Bridge.class, ChronService.class, DPNConfig.class})
public class Application implements CommandLineRunner {

    private final ChronService service;

    @Autowired
    public Application(ChronService service) {
        this.service = service;
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(Application.class);
        application.addListeners(new ApplicationPidFileWriter());
        SpringApplication.exit(application.run(args));
    }

    @Override
    public void run(String... strings) {
        service.run();
    }

}
