package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.backend.OutdatedVersionException;
import edu.uw.zookeeper.safari.peer.protocol.SafariExceptionSerializer.SafariErrorCode;

public class SafariExceptionDeserializer extends StdDeserializer<SafariException> {

    public static SafariExceptionDeserializer create() {
        return new SafariExceptionDeserializer();
    }
    
    private static final long serialVersionUID = 1L;

    protected SafariExceptionDeserializer() {
        super(SafariException.class);
    }

    @Override
    public SafariException deserialize(JsonParser json, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        if (json.getCurrentToken() != JsonToken.START_OBJECT) {
            throw ctxt.wrongTokenException(json, JsonToken.START_OBJECT, "");
        }
        VersionedId version = null;
        while (json.nextToken() != JsonToken.END_OBJECT) {
            if (json.getCurrentToken() != JsonToken.FIELD_NAME) {
                throw ctxt.wrongTokenException(json, JsonToken.FIELD_NAME, "");
            }
            final String fieldName = json.getText();
            if (fieldName.equals("error")) {
                if (json.nextToken() != JsonToken.VALUE_NUMBER_INT) {
                    throw ctxt.wrongTokenException(json, JsonToken.VALUE_NUMBER_INT, fieldName);
                }
                if (SafariErrorCode.valueOf(json.getIntValue()) != SafariErrorCode.SAFARI_OUTDATED_VERSION_ERROR) {
                    throw ctxt.weirdNumberException(json.getIntValue(), SafariErrorCode.class, fieldName);
                }
            } else if (fieldName.equals("version")) {
                json.nextToken();
                version = json.readValueAs(VersionedId.class);
            } else {
                throw ctxt.weirdStringException(fieldName, OutdatedVersionException.class, "");
            }
        }
        return new OutdatedVersionException(checkNotNull(version));
    }
    
    @Override
    public boolean isCachable() { 
        return true; 
    }
}
