package org.chronopolis.ingest.processor;

import org.chronopolis.amqp.ChronProducer;
import org.chronopolis.db.common.RestoreRepository;
import org.chronopolis.db.common.model.RestoreRequest;
import org.chronopolis.ingest.config.IngestSettings;
import org.chronopolis.messaging.Indicator;
import org.chronopolis.messaging.base.ChronMessage;
import org.chronopolis.messaging.base.ChronProcessor;
import org.chronopolis.messaging.collection.CollectionRestoreReplyMessage;
import org.chronopolis.messaging.factory.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Created by shake on 8/7/14.
 */
public class CollectionRestoreReplyProcessor implements ChronProcessor {
    private final Logger log = LoggerFactory.getLogger(CollectionRestoreReplyProcessor.class);

    private final IngestSettings settings;
    private final ChronProducer producer;
    private final MessageFactory messageFactory;
    private final RestoreRepository restoreRepository;

    public CollectionRestoreReplyProcessor(final IngestSettings settings,
                                           final ChronProducer producer,
                                           final MessageFactory messageFactory,
                                           final RestoreRepository restoreRepository) {
        this.settings = settings;
        this.producer = producer;
        this.messageFactory = messageFactory;
        this.restoreRepository = restoreRepository;
    }

    @Override
    public void process(final ChronMessage chronMessage) {
        if (!(chronMessage instanceof CollectionRestoreReplyMessage)) {
            throw new RuntimeException("Invalid message for "
                    + this.getClass().getName()
                    + ": "
                    + chronMessage.getClass().getName());
        }

        CollectionRestoreReplyMessage msg =
                (CollectionRestoreReplyMessage) chronMessage;

        RestoreRequest restoreRequest = restoreRepository.findByCorrelationId(msg.getCorrelationId());

        ChronMessage reply = null;
        StringBuilder location = new StringBuilder();
        location.append(settings.getExternalUser())
                .append("@")
                .append(settings.getStorageServer())
                .append(":");

        Path restore = Paths.get(settings.getRestore(), restoreRequest.getDirectory());
        location.append(restore);

        // For now, we'll just pull from ourselves
        // In the future we'll want a way to actually choose a node
        if (msg.getOrigin().equals(settings.getNode())
                && Indicator.ACK.name().equalsIgnoreCase(msg.getMessageAtt())) {
            reply = messageFactory.collectionRestoreLocationMessage("rsync",
                    location.toString(),
                    Indicator.ACK,
                    chronMessage.getCorrelationId());
        } else {
            reply = messageFactory.collectionRestoreLocationMessage(null,
                    null,
                    Indicator.NAK,
                    chronMessage.getCorrelationId());
        }

        producer.send(reply, msg.getReturnKey());
    }
}
