package edu.uw.zookeeper.orchestra.peer.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ConnectMessage;

public abstract class OpenSessionMessage<V extends ConnectMessage<?>> extends EncodableMessage<Long, V> {

    protected OpenSessionMessage(
            Long identifier,
            V value) {
        super(identifier, value);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("identifier", Session.toString(getIdentifier()))
                .add("value", getValue())
                .toString();
    }
}
