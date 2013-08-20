package edu.uw.zookeeper.orchestra.peer.protocol;

@MessageBodyType(MessageType.MESSAGE_TYPE_HEARTBEAT)
public class MessageHeartbeat implements MessageBody {
    public MessageHeartbeat() {}
}
