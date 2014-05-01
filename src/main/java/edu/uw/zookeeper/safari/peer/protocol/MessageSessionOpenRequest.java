package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.proto.Records;

@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_OPEN_REQUEST)
public class MessageSessionOpenRequest extends OpenSessionMessage<ConnectMessage.Request> {

    public static MessageSessionOpenRequest of(
            Long identifier,
            ConnectMessage.Request message) {
        return new MessageSessionOpenRequest(identifier, message);
    }
    
    @JsonCreator
    public MessageSessionOpenRequest(
            @JsonProperty("identifier") Long identifier,
            @JsonProperty("message") Records.Request message) {
        super(identifier, (ConnectMessage.Request) message);
    }
}
