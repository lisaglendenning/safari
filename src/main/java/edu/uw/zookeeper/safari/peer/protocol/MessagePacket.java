package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import edu.uw.zookeeper.common.Pair;

public class MessagePacket extends Pair<MessageHeader, MessageBody> {

    public static <T extends MessageBody> MessagePacket of(T second) {
        MessageHeader first = MessageHeader.of(MessageTypes.typeOf(second.getClass()));
        return of(first, second);
    }
    
    public static MessagePacket of(MessageHeader first, MessageBody second) {
        return new MessagePacket(first, second);
    }
    
    public MessagePacket(MessageHeader first, MessageBody second) {
        super(checkNotNull(first), checkNotNull(second));
    }
    
    public <T extends MessageBody> T getBody(Class<T> cls) {
        return cls.cast(second());
    }
}
