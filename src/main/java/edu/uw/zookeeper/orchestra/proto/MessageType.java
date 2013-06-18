package edu.uw.zookeeper.orchestra.proto;

public enum MessageType {
    MESSAGE_TYPE_NONE,
    MESSAGE_TYPE_HANDSHAKE,
    MESSAGE_TYPE_SESSION;
    
    public static MessageType valueOf(int value) {
        return MessageType.values()[value];
    }
    
    public int intValue() {
        return ordinal();
    }
}