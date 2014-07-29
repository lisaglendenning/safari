package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.backend.OutdatedVersionException;

public class ShardedResponseMessageDeserializer extends StdDeserializer<ShardedResponseMessage<?>> {

    public static ShardedResponseMessageDeserializer create() {
        return new ShardedResponseMessageDeserializer();
    }
    
    private static final long serialVersionUID = 1L;

    protected ShardedResponseMessageDeserializer() {
        super(ShardedResponseMessage.class);
    }

    @Override
    public ShardedResponseMessage<?> deserialize(JsonParser json, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        if (json.getCurrentToken() != JsonToken.START_OBJECT) {
            throw ctxt.wrongTokenException(json, JsonToken.START_OBJECT, "");
        }
        if (json.nextToken() != JsonToken.FIELD_NAME) {
            throw ctxt.wrongTokenException(json, JsonToken.FIELD_NAME, "");
        }
        String fieldName = json.getText();
        if (fieldName != "shard") {
            throw ctxt.weirdStringException(fieldName, ShardedResponseMessage.class, "");
        }
        json.nextToken();
        final VersionedId shard = json.readValueAs(VersionedId.class);
        
        final ShardedResponseMessage<?> value;
        if (json.nextToken() != JsonToken.FIELD_NAME) {
            throw ctxt.wrongTokenException(json, JsonToken.FIELD_NAME, "");
        }
        fieldName = json.getText();
        if (fieldName.equals("response")) {
            json.nextToken();
            final Message.ServerResponse<?> response = json.readValueAs(Message.ServerResponse.class);
            value = ShardedServerResponseMessage.valueOf(shard, response);
        } else if (fieldName.equals("error")) {
            json.nextToken();
            final ShardedErrorResponseMessage.ErrorResponse error = json.readValueAs(ShardedErrorResponseMessage.ErrorResponse.class);
            value = ShardedErrorResponseMessage.valueOf(shard, error);
        } else {
            throw ctxt.weirdStringException(fieldName, OutdatedVersionException.class, "");
        }
        if (json.nextToken() != JsonToken.END_OBJECT) {
            throw ctxt.wrongTokenException(json, JsonToken.END_OBJECT, "");
        }
        return value;
    }
    
    @Override
    public boolean isCachable() { 
        return true; 
    }
}
