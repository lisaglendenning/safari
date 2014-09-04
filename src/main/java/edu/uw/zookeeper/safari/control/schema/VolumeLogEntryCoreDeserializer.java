package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;

import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.jackson.JacksonCoreDeserializer;
import edu.uw.zookeeper.safari.schema.volumes.BoundVolumeOperator;

public class VolumeLogEntryCoreDeserializer implements JacksonCoreDeserializer<VolumeLogEntry<?>> {

    public static VolumeLogEntryCoreDeserializer create() {
        return new VolumeLogEntryCoreDeserializer();
    }

    protected VolumeLogEntryCoreDeserializer() {}

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends VolumeLogEntry> handledType() {
        return VolumeLogEntry.class;
    }

    @Override
    public VolumeLogEntry<?> deserialize(JsonParser json) throws IOException,
            JsonProcessingException {
        JsonToken token = json.getCurrentToken();
        if (token == null) {
            token = json.nextToken();
            if (token == null) {
                return null;
            }
        }
        VolumeLogEntry<?> value;
        switch (token) {
        case VALUE_STRING:
        {
            RelativeZNodePath path = RelativeZNodePath.fromString(json.getText());
            value = LinkVolumeLogEntry.create(path);
            break;
        }
        case START_ARRAY:
        {
            BoundVolumeOperator<?> operation = json.readValueAs(BoundVolumeOperator.class);
            value = OperatorVolumeLogEntry.create(operation);
            break;
        }
        default:
            throw new JsonParseException(String.valueOf(json.getCurrentToken()), json.getCurrentLocation());
        }
        return value;
    }
}
