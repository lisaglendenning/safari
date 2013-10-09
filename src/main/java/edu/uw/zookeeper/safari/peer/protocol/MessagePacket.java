package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import edu.uw.zookeeper.common.AbstractPair;

public class MessagePacket<T extends MessageBody> extends AbstractPair<MessageHeader, T> {

    public static <T extends MessageBody> MessagePacket<T> of(T body) {
        MessageHeader header = MessageHeader.of(MessageTypes.typeOf(body.getClass()));
        return of(header, body);
    }
    
    public static <T extends MessageBody> MessagePacket<T> of(MessageHeader header, T body) {
        return new MessagePacket<T>(header, body);
    }
    
    public MessagePacket(MessageHeader header, T body) {
        super(checkNotNull(header), checkNotNull(body));
    }
    
    public MessageHeader getHeader() {
        return first;
    }
    
    public MessageBody getBody() {
        return second;
    }
}
