/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.ingest;

import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.db.DatabaseManager;
import org.chronopolis.db.model.CollectionIngest;
import org.chronopolis.ingest.config.IngestConfiguration;
import org.chronopolis.ingest.config.IngestJPAConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 *
 * @author shake
 */
public class IngestConsumer {
    
    private static String readLine() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            return reader.readLine();
        } catch (IOException ex) {
            throw new RuntimeException("Can't read from STDIN");
        }
    }
    
    public static void main(String [] args) {
        AnnotationConfigApplicationContext context2 = new AnnotationConfigApplicationContext();
        context2.register(IngestJPAConfiguration.class);
        context2.register(IngestConfiguration.class);
        context2.refresh();

        boolean done = false;
        ChronProducer p = (ChronProducer) context2.getBean("producer");
        IngestProperties props = (IngestProperties) context2.getBean(IngestProperties.class);

        while (!done) {
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            
            System.out.println("Enter 'q' to exit: ");
            if ("q".equalsIgnoreCase(readLine())) {
                System.out.println("Shutting down");
                done = true;
            }
        }
        
        context2.close();
        System.out.println("Closed for business");
    }
}
