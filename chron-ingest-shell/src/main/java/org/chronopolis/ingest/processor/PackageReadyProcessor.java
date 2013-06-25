/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.ingest.processor;

import org.chronopolis.messaging.base.ChronMessage2;
import org.chronopolis.messaging.base.ChronProcessor;
import org.chronopolis.messaging.pkg.PackageReadyMessage;
import org.chronopolis.transfer.FileTransfer;

/**
 *
 * @author shake
 */
public class PackageReadyProcessor implements ChronProcessor {

    @Override
    public void process(ChronMessage2 chronMessage) {
        if ( !(chronMessage instanceof PackageReadyMessage)) {
            // Error out
        }
        // Things to do:
        // 1: Validate message
        // 2: Grab bag
        // 3: validate and create token store
        
        // String protocol = getProtocol();
        FileTransfer transferObj = null;
        
        /*
        if (protocol.equals("rsync")) {
            transferObj = new RSyncTransfer();
        } else if (protocol.equals("https")) {
            transferObj = new HttpsTransfer();
        } else {
            // Unsupported protocol
        }
        */
        
        // Should end up being the location for a download
        String tokenStore = "https://chron-monitor.umiacs.umd.edu/tokenStore001";
        
        // Sending the next message will be done in the ingest consumer?
        // CollectionInitMessage collectionInitRequest = new CollectionInitMessage();
        // collectionInitRequest.setAuditPeriod("somedefinedperiod");
        // collectionInitRequest.setCollection(getPackageName());
        // collectionInitRequest.setDepositor(getDepositor());
        // collectionInitRequest.setTokenStore(tokenStore);
        
        // Send message
    }
    
}