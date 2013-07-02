package edu.uw.zookeeper.orchestra.protocol;

import com.google.common.base.Objects;

public abstract class MessageSessionBody extends MessageBody {

    protected final long sessionId;

    public MessageSessionBody(long sessionId) {
        super();
        this.sessionId = sessionId;
    }

    public long getSessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("sessionId", getSessionId()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (obj.getClass() != getClass())) {
            return false;
        }
        MessageSessionBody other = (MessageSessionBody) obj;
        return Objects.equal(getSessionId(), other.getSessionId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getSessionId());
    }
}
