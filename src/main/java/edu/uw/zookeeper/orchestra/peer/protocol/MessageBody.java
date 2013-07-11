package edu.uw.zookeeper.orchestra.peer.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.Maps;

public abstract class MessageBody {
    
    protected final static BiMap<MessageType, Class<? extends MessageBody>> types 
        = Maps.synchronizedBiMap(EnumHashBiMap.<MessageType, Class<? extends MessageBody>>create(
                        MessageType.class));

    public static Class<? extends MessageBody> register(Class<? extends MessageBody> cls) {
        MessageBodyType annotation = cls.getAnnotation(MessageBodyType.class);
        checkArgument(annotation != null, cls);
        return types.put(annotation.type(), cls);
    }
    
    public static Class<? extends MessageBody> registerType(MessageType type, Class<? extends MessageBody> cls) {
        return types.put(type, cls);
    }

    public static MessageType unregister(Class<? extends MessageBody> cls) {
        return types.inverse().remove(cls);
    }
    
    public static Class<? extends MessageBody> unregisterType(MessageType type) {
        return types.remove(type);
    }
    
    public static Class<? extends MessageBody> registeredType(MessageType type) {
        return types.get(type);
    }
    
    public static MessageType typeOf(Class<? extends MessageBody> cls) {
        return types.inverse().get(cls);
    }
}
