package edu.uw.zookeeper.orchestra.proto;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION)
public abstract class SessionMessage extends MessageBody {

    public abstract SessionHeader getHeader();
    public abstract byte[] getPayload();
}
