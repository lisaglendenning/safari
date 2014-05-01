package edu.uw.zookeeper.safari.peer.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ConnectMessage;

public abstract class OpenSessionMessage<V extends ConnectMessage<?>> extends ValueMessage<Long, V> {

    protected OpenSessionMessage(
            Long identifier,
            V message) {
        super(identifier, message);
    }
    
    public Long getIdentifier() {
        return identifier;
    }
    
    public V getMessage() {
        return value;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("identifier", Session.toString(identifier))
                .add("message", value)
                .toString();
    }
}
