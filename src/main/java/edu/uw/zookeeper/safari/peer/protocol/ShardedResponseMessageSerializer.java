package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;


public class ShardedResponseMessageSerializer extends StdSerializer<ShardedResponseMessage<?>> {

    public static ShardedResponseMessageSerializer create() {
        return new ShardedResponseMessageSerializer();
    }

    protected ShardedResponseMessageSerializer() {
        super(ShardedResponseMessage.class, true);
    }

    @Override
    public void serialize(ShardedResponseMessage<?> value, JsonGenerator json, SerializerProvider provider) throws JsonGenerationException, IOException {
        json.writeStartObject();
        json.writeFieldName("shard");
        json.writeObject(value.getShard());
        if (value instanceof ShardedServerResponseMessage<?>) {
            json.writeFieldName("response");
            json.writeObject(((ShardedServerResponseMessage<?>) value).getResponse());
        } else {
            json.writeFieldName("error");
            json.writeObject(((ShardedErrorResponseMessage) value).getError());
        }
        json.writeEndObject();
    }
    
    public static enum SafariErrorCode {
        SAFARI_OUTDATED_VERSION_ERROR;
        
        public static SafariErrorCode valueOf(int value) {
            if (value == SAFARI_OUTDATED_VERSION_ERROR.intValue()) {
                return SAFARI_OUTDATED_VERSION_ERROR;
            }
            return null;
        }
        
        public int intValue() {
            return ordinal();
        }
    }
}
