package org.chronopolis.intake.duracloud.model;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Serialize BaggingHistory objects in a way that the bridge can understand
 *
 * Created by shake on 11/13/15.
 */
public class BaggingHistorySerializer implements JsonSerializer<BaggingHistory> {
    @Override
    public JsonElement serialize(BaggingHistory baggingHistory, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject object = new JsonObject();
        // serialize the history
        JsonElement serial = jsonSerializationContext.serialize(baggingHistory.getHistory());
        object.add("history", new JsonPrimitive(serial.toString()));
        object.add("alternate", new JsonPrimitive(baggingHistory.getAlternate()));
        return object;
    }
}
