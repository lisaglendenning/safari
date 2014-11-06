package edu.uw.zookeeper.safari.peer.protocol;

public enum MessageType {
    MESSAGE_TYPE_NONE,
    MESSAGE_TYPE_HEARTBEAT,
    MESSAGE_TYPE_HANDSHAKE,
    MESSAGE_TYPE_XOMEGA,
    MESSAGE_TYPE_SESSION_OPEN_REQUEST,
    MESSAGE_TYPE_SESSION_OPEN_RESPONSE,
    MESSAGE_TYPE_SESSION_REQUEST,
    MESSAGE_TYPE_SESSION_RESPONSE;
    
    public static MessageType valueOf(int value) {
        return MessageType.values()[value];
    }
    
    public int intValue() {
        return ordinal();
    }
}