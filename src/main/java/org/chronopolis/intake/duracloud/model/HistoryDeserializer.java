package org.chronopolis.intake.duracloud.model;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.chronopolis.intake.duracloud.remote.model.History;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

/**
 * Top level history deserializer class which delegates to other
 * deserializers based on the snapshot-action
 *
 * Created by shake on 2/23/16.
 */
public class HistoryDeserializer implements JsonDeserializer<History> {

    private final Logger log = LoggerFactory.getLogger(HistoryDeserializer.class);

    @Override
    public History deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        ImmutableMap<String, Type> typeMap = ImmutableMap.<String, Type>builder()
                .put("SNAPSHOT_BAGGED", BaggingHistory.class)
                .build();

        JsonArray array = jsonElement.getAsJsonArray();
        JsonObject actionObject = array.get(0).getAsJsonObject();
        if (actionObject.has("snapshot-action")) {
            String action = actionObject.getAsJsonPrimitive("snapshot-action").getAsString();
            log.info("Found snapshot-action {}", action);
            return jsonDeserializationContext.deserialize(jsonElement, typeMap.get(action));
        }

        return null;
    }
}
