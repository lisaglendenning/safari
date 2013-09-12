package edu.uw.zookeeper.safari.peer.protocol;

@MessageBodyType(MessageType.MESSAGE_TYPE_HEARTBEAT)
public enum MessageHeartbeat implements MessageBody {
    MESSAGE_HEARTBEAT;
    
    public static MessageHeartbeat getInstance() {
        return MESSAGE_HEARTBEAT;
    }
}
