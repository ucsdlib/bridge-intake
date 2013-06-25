/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.messaging.file;

import org.chronopolis.messaging.MessageType;
import org.chronopolis.messaging.base.ChronBody;
import org.chronopolis.messaging.base.ChronHeader;
import org.chronopolis.messaging.base.ChronMessage2;

/**
 *
 * @author shake
 */
public class FileQueryResponseMessage extends ChronMessage2 {
    protected MessageType type;
    private final String DEPOSITOR_KEY = "depositor";
    private final String PROTOCOL_KEY = "protocol";
    private final String FILENAME_KEY = "filename";
    private final String LOCATION_KEY = "location";

    public FileQueryResponseMessage() {
        super(MessageType.FILE_QUERY_RESPONSE);
        this.body = new ChronBody(type);
        this.header = new ChronHeader();
    }
    

}