package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.proto.Records;

@MessageBodyType(MessageType.MESSAGE_TYPE_SESSION_OPEN_RESPONSE)
public class MessageSessionOpenResponse extends OpenSessionMessage<ConnectMessage.Response> {

    public static MessageSessionOpenResponse of(
            Long identifier,
            ConnectMessage.Response message) {
        return new MessageSessionOpenResponse(identifier, message);
    }
    
    @JsonCreator
    public MessageSessionOpenResponse(
            @JsonProperty("identifier") Long identifier,
            @JsonProperty("message") Records.Response message) {
        super(identifier, (ConnectMessage.Response) message);
    }
}
