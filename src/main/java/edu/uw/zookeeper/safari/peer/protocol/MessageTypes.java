package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public abstract class MessageTypes {
    
    protected final static BiMap<MessageType, Class<? extends MessageBody>> types 
        = Maps.synchronizedBiMap(EnumHashBiMap.<MessageType, Class<? extends MessageBody>>create(
                        MessageType.class));

    public static Class<? extends MessageBody> register(Class<? extends MessageBody> cls) {
        MessageBodyType annotation = cls.getAnnotation(MessageBodyType.class);
        checkArgument(annotation != null, cls);
        return types.put(annotation.value(), cls);
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
    
    public static Set<Entry<MessageType,Class<? extends MessageBody>>> registeredTypes() {
        synchronized (types) {
            return ImmutableSet.copyOf(types.entrySet());
        }
    }

    static {
        for (Class<? extends MessageBody> cls: ImmutableList.of(
                MessageHandshake.class,
                MessageHeartbeat.class,
                MessageSessionOpenRequest.class,
                MessageSessionOpenResponse.class,
                MessageSessionRequest.class,
                MessageSessionResponse.class)) {
            MessageTypes.register(cls);
        }
    }
}
