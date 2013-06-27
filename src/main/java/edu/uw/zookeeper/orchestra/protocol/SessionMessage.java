package edu.uw.zookeeper.orchestra.protocol;

@MessageBodyType(type=MessageType.MESSAGE_TYPE_SESSION)
public abstract class SessionMessage extends MessageBody {

    public abstract SessionHeader getHeader();
    public abstract byte[] getPayload();
}
