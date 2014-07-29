package edu.uw.zookeeper.safari.peer.protocol;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.backend.OutdatedVersionException;

/**
 * Currently only OutdatedVersionException is sent in messages.
 */
public class SafariExceptionSerializer extends StdSerializer<SafariException> {

    public static SafariExceptionSerializer create() {
        return new SafariExceptionSerializer();
    }

    protected SafariExceptionSerializer() {
        super(SafariException.class);
    }

    @Override
    public void serialize(SafariException value, JsonGenerator json, SerializerProvider provider) throws JsonGenerationException, IOException {
        OutdatedVersionException exception = (OutdatedVersionException) value;
        json.writeStartObject();
        json.writeFieldName("error");
        json.writeNumber(SafariErrorCode.SAFARI_OUTDATED_VERSION_ERROR.intValue());
        json.writeFieldName("version");
        json.writeObject(exception.getVersion());
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
