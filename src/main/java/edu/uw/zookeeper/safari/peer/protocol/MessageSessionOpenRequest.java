package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.proto.Records;

@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_OPEN_REQUEST)
public class MessageSessionOpenRequest extends OpenSessionMessage<ConnectMessage.Request> {

    public static MessageSessionOpenRequest of(
            Long identifier,
            ConnectMessage.Request value) {
        return new MessageSessionOpenRequest(identifier, value);
    }
    
    @JsonCreator
    public MessageSessionOpenRequest(
            @JsonProperty("identifier") Long identifier,
            @JsonProperty("value") Records.Request value) {
        this(identifier, (ConnectMessage.Request) value);
    }

    public MessageSessionOpenRequest(
            Long identifier,
            ConnectMessage.Request value) {
        super(identifier, value);
    }
}
