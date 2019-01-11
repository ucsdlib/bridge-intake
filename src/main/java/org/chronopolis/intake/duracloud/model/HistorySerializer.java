package org.chronopolis.intake.duracloud.model;

import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.chronopolis.intake.duracloud.remote.model.History;

import java.lang.reflect.Type;

/**
 *
 * Created by shake on 2/19/16.
 */
public class HistorySerializer implements JsonSerializer<History> {

    @Override
    public JsonElement serialize(History history,
                                 Type type,
                                 JsonSerializationContext context) {
        return context.serialize(history, history.getClass());
    }
}
