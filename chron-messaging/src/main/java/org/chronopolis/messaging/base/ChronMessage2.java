/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.messaging.base;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.chronopolis.messaging.MessageType;
import org.chronopolis.messaging.collection.CollectionInitMessage;
import org.chronopolis.messaging.file.FileQueryMessage;
import org.chronopolis.messaging.file.FileQueryResponseMessage;
import org.chronopolis.messaging.pkg.PackageIngestCompleteMessage;
import org.chronopolis.messaging.pkg.PackageIngestStatusQueryMessage;
import org.chronopolis.messaging.pkg.PackageIngestStatusResponseMessage;
import org.chronopolis.messaging.pkg.PackageReadyMessage;

import static org.chronopolis.messaging.MessageConstant.CORRELATION_ID;
import static org.chronopolis.messaging.MessageConstant.ORIGIN;
import static org.chronopolis.messaging.MessageConstant.DATE;
import static org.chronopolis.messaging.MessageConstant.RETURN_KEY;
import org.chronopolis.messaging.collection.CollectionInitCompleteMessage;

/**
 * I got confused with some of the other classes so I made this package to
 * do testing in. This class represents what a message consists of -- the Type
 * for enforcing the correct args, the header, and the body.
 * @author shake
 */
/*
 * TODO: Make MessageType static in each type of message
 * TODO: Create class prototypes here
 * TODO: Header sets in this class too
 * TODO: Do we need to encapsulate the Header? Why not just have the vals in this class?
 *       What was I even thinking?
 */
public class ChronMessage2 {
    // protected ChronHeader header;
    protected ChronBody body;
    protected final MessageType type;

    // Headers
    private String origin;
    private String returnKey;
    private String correlationId;
    private String date;

    public ChronMessage2(MessageType type) {
        this.type = type;
    }
    
    public void setHeader(Map<String, Object> header) {
        this.origin = (String) header.get(ORIGIN.toString());
        this.returnKey= (String) header.get(RETURN_KEY.toString());
        this.correlationId = (String) header.get(CORRELATION_ID.toString());
        this.date = (String) header.get(DATE.toString());
    }

    public void setBody(ChronBody body) {
        this.body = new ChronBody(type, body);
    }

    public final Map<String, Object> getHeader() {
        HashMap<String, Object> header = new HashMap<>();
        header.put(ORIGIN.toString(), origin);
        header.put(RETURN_KEY.toString(), returnKey);
        header.put(CORRELATION_ID.toString(), correlationId);
        header.put(DATE.toString(), date);
        return header;
    }

    public final ChronBody getChronBody() {
        return body;
    }

    public MessageType getType() { 
        return this.type;
    }

    /*
     * We want this method to return a byte array for use by RabbitMQ. The byte
     * array consists ObjectOutputStream with a ChronBody as the data. In addition,
     * the CorrelationID and Timestamp should be generated at this time for
     * the ChronHeader
     *
     */
    public final byte[] createMessage() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(body);
        // header.setDate(new Date().toString());
        if ( correlationId == null ) {
            setCorrelationId(UUID.randomUUID().toString());
        }
        return baos.toByteArray();
    }

    // Helper for returning the type of message we want
    public static ChronMessage2 getMessage(MessageType type) {
        switch (type) {
            case FILE_QUERY:
                return new FileQueryMessage(); 
            case FILE_QUERY_RESPONSE:
                return new FileQueryResponseMessage();
            case COLLECTION_INIT:
                return new CollectionInitMessage();
            case COLLECTION_INIT_COMPLETE:
                return new CollectionInitCompleteMessage();
            case PACKAGE_INGEST_READY:
                return new PackageReadyMessage();
            case PACKAGE_INGEST_COMPLETE:
                return new PackageIngestCompleteMessage();
            case PACKAGE_INGEST_STATUS_QUERY:
                return new PackageIngestStatusQueryMessage();
            case PACKAGE_INGEST_STATUS_RESPONSE:
                return new PackageIngestStatusResponseMessage();

            default:
                throw new UnexpectedMessageTypeException(type.toString());

        }
    }

    private static class UnexpectedMessageTypeException extends RuntimeException {
        public UnexpectedMessageTypeException(String toString) {
            super(toString);
        }
    }

    // Header methods 
    public final void setOrigin(String origin) {
        this.origin = origin;
    }

    public final void setReturnKey(String returnKey) {
        this.returnKey = returnKey;
    }

    public final void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public final void setDate(String date) {
        this.date = date;
    }

    public final String getOrigin() {
        return origin;
    }

    public final String getReturnKey() {
        return returnKey;
    }

    public final String getCorrelationId() {
        return correlationId;
    }

    public final String getDate() {
        return date;
    }
}
