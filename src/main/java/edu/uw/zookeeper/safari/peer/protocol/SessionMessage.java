package edu.uw.zookeeper.safari.peer.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Session;

public abstract class SessionMessage<V> extends ValueMessage<Long, V> {

    protected SessionMessage(
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