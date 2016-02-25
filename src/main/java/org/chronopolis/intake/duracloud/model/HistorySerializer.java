package org.chronopolis.intake.duracloud.model;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

/**
 *
 * Created by shake on 2/19/16.
 */
public class HistorySerializer implements JsonSerializer<History> {
    private final Logger log = LoggerFactory.getLogger(HistorySerializer.class);

    @Override
    public JsonElement serialize(History history, Type type, JsonSerializationContext jsonSerializationContext) {
        log.debug("Serializing history of type {}", history.getClass().getName());
        return jsonSerializationContext.serialize(history, history.getClass());
    }
}
