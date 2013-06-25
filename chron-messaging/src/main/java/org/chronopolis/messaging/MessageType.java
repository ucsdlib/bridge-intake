/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.messaging;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Container for chronopolis/DPN style messages
 * MessageState - The state of the node who produces the message
 * ProcessType - The process type of the message flow
 * Stage -
 * Indicator - The type of message
 * Args - The parameters of the message
 *
 * @author toaster
 */
public enum MessageType {
    
    // DPN Messages
    INGEST_INIT_QUERY(MessageState.ORIGIN, ProcessType.INGEST, "init", Indicator.QUERY, "size", "protocol"),
    INGEST_AVAIL_ACK(MessageState.RESPONSE, ProcessType.INGEST, "avail", Indicator.ACK),
    INGEST_AVAIL_NAK(MessageState.RESPONSE, ProcessType.INGEST, "avail", Indicator.NAK),
    // Ingest <--> Distribute Messages,
    // Do these messages need a query and response? There is only an ack associated
    // with them
    DISTRIBUTE_COLL_INIT(MessageState.ORIGIN, ProcessType.DISTRIBUTE, "init", Indicator.QUERY,
            "depositor", "collection", "tokenStore", "audit.period"),
    // Distribute <--> Distribute,
    // Will query other nodes to ask for files and get a response
    FILE_QUERY(MessageState.ORIGIN, ProcessType.QUERY, "avail", Indicator.QUERY,
            "depositor", "protocol", "location", "filename"),
    FILE_QUERY_RESPONSE(MessageState.RESPONSE, ProcessType.QUERY, "transfer", Indicator.ACK,
            "available", "protocol", "location"),
    PACKAGE_INGEST_READY(MessageState.ORIGIN, ProcessType.INGEST, "init", Indicator.QUERY, 
            "package-name", "location", "depositor", "size"),
    PACKAGE_INGEST_COMPLETE(MessageState.RESPONSE, ProcessType.INGEST, "fin", Indicator.ACK,
            "status", "failed-items"),
    PACKAGE_INGEST_STATUS_QUERY(MessageState.RESPONSE, ProcessType.INGEST, "query", Indicator.ACK,
            "package-name", "depositor"), 
    PACKAGE_INGEST_STATUS_RESPONSE(MessageState.RESPONSE, ProcessType.INGEST, "response", Indicator.ACK,
            "status", "completion_percent"), 

    // Deprecated Messages
    @Deprecated
    DISTRIBUTE_INIT_ACK(MessageState.RESPONSE, ProcessType.DISTRIBUTE, "fin", Indicator.ACK),
    @Deprecated
    DISTRIBUTE_TRANSFER_REQUEST(MessageState.ORIGIN, ProcessType.DISTRIBUTE, "transfer", Indicator.QUERY,
            "depositor", "filename", "digest-type", "digest", "location"),
    @Deprecated
    DISTRIBUTE_TRANSFER_ACK(MessageState.ORIGIN, ProcessType.DISTRIBUTE, "complete", Indicator.ACK),
    @Deprecated
    DISTRIBUTE_TRANSFER_NAK(MessageState.ORIGIN, ProcessType.DISTRIBUTE, "complete", Indicator.NAK),
    ;
    private MessageState state;
    private ProcessType process;
    private String stage;
    private Indicator indicator;
    private List<String> args;
    
    private MessageType(MessageState state, ProcessType process, String stage, Indicator indicator, String... args) {
        this.state = state;
        this.process = process;
        this.stage = stage;
        this.indicator = indicator;
        this.args = Collections.unmodifiableList(Arrays.asList(args));
    }
    
    public static MessageType decode(String message) {
        if (null == message) {
            throw new NullPointerException();
        }
        
        switch (message.toLowerCase()) {
            case "o-ingest-init-query":
                return INGEST_INIT_QUERY;
            case "r-ingest-avail-ack":
                return INGEST_AVAIL_ACK;
            case "r-ingest-avail-nak":
                return INGEST_AVAIL_NAK;
            case "o-distribute-coll-init":
                return DISTRIBUTE_COLL_INIT;
            case "r-distribute-init-ack":
                return DISTRIBUTE_INIT_ACK;
            case "o-distribute-transfer-request":
                return DISTRIBUTE_TRANSFER_REQUEST;
            case "r-distribute-transfer-ack":
                return DISTRIBUTE_TRANSFER_ACK;
            case "r-distribute-transfer-nak":
                return DISTRIBUTE_TRANSFER_NAK;
            default:
                throw new IllegalArgumentException("unknown message name: " + message);
                
        }
    }
    
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(state.getName()).append('-');
        sb.append(process.getName()).append('-');
        sb.append(stage).append('-');
        sb.append(indicator.getName());
        return sb.toString();
    }
    
    /**
     *
     * @return
     */
    public MessageState getState() {
        return state;
    }
    
    /**
     * List of acceptable argument keys for this message type
     * @return
     */
    public List<String> getArgs() {
        return args;
    }
    
    /**
     * Each of our control flows will have a unique identifier attached to it.  For example, Content Ingest
     * will be "ingest".  Every message exchanged during the control flow / process will be has this tag.
     * ProcessType
     * @return
     */
    public ProcessType getProcess() {
        return process;
    }
    
    /**
     * Refers to the point at which the message is sent in the process.  Unambiguously replaces sequence, and
     * does not lend itself to being implemented with a += 1, a method that provided no benefit.  ProcessType
     * @return
     */
    public String getStage() {
        return stage;
    }
    
    /**
     * ack/nack/query
     * @return
     */
    public Indicator getIndicator() {
        return indicator;
    }
}