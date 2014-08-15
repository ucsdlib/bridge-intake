/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.ingest.processor;

import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.common.mail.MailUtil;
import org.chronopolis.db.DatabaseManager;
import org.chronopolis.db.ingest.IngestDB;
import org.chronopolis.db.ingest.ReplicationFlowTable;
import org.chronopolis.db.model.CollectionIngest;
import org.chronopolis.db.model.ReplicationFlow;
import org.chronopolis.db.model.ReplicationState;
import org.chronopolis.messaging.base.ChronMessage;
import org.chronopolis.messaging.base.ChronProcessor;
import org.chronopolis.messaging.collection.CollectionInitCompleteMessage;
import org.chronopolis.messaging.exception.InvalidMessageException;
import org.chronopolis.messaging.factory.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author shake
 */
public class CollectionInitCompleteProcessor implements ChronProcessor {
    private final Logger log = LoggerFactory.getLogger(CollectionInitCompleteProcessor.class);

    private final ChronProducer producer;
    private final MessageFactory messageFactory;
    private final DatabaseManager manager;
    private final MailUtil mailUtil;

    public CollectionInitCompleteProcessor(ChronProducer producer,
                                           MessageFactory messageFactory,
                                           DatabaseManager manager,
                                           MailUtil mailUtil) {
        this.producer = producer;
        this.messageFactory = messageFactory;
        this.manager = manager;
        this.mailUtil = mailUtil;
    }

    @Override
    public void process(ChronMessage chronMessage) {
        if (!(chronMessage instanceof CollectionInitCompleteMessage)) {
            // Error out
            log.error("Invalid message type");
            throw new InvalidMessageException("Expected message of type CollectionInitComplete"
                    + " but received " + chronMessage.getClass().getName());
        }
        CollectionInitCompleteMessage message = (CollectionInitCompleteMessage) chronMessage;

        ChronMessage response = messageFactory.DefaultPackageIngestCompleteMessage();

        IngestDB db = manager.getIngestDatabase();
        CollectionIngest ci = db.findByCorrelationId(chronMessage.getCorrelationId());
        log.info("Retrieved item correlation {} and toDpn value of {}",
                ci.getCorrelationId(), ci.getToDpn());

        ReplicationFlowTable flowTable = manager.getReplicationFlowTable();
        ReplicationFlow flow  = flowTable.findByNodeAndCorrelationId(
                chronMessage.getOrigin(),
                chronMessage.getCorrelationId());
        flow.setCurrentState(ReplicationState.FINISHED);
        flowTable.save(flow);

        // Once again, hold the routing key temporarily
        // We don't actually send this until all nodes have finished replicating
        producer.send(response, "package.intake.umiacs");
    }

}
