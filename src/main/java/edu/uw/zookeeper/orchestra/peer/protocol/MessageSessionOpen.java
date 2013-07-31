package edu.uw.zookeeper.orchestra.peer.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.ConnectMessage;

public abstract class MessageSessionOpen<V extends ConnectMessage<?>> extends EncodableMessage<V> {

    private final long sessionId;
    
    protected MessageSessionOpen(
            long sessionId,
            V record) {
        super(record);
        this.sessionId = sessionId;
    }
    
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sesisonId", getSessionId())
                .add("delegate", delegate())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != getClass())) {
            return false;
        }
        MessageSessionOpen<?> other = (MessageSessionOpen<?>) obj;
        return Objects.equal(getSessionId(), other.getSessionId())
                && Objects.equal(delegate(), other.delegate());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId(), delegate());
    }
}
