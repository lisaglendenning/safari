package edu.uw.zookeeper.orchestra.peer.protocol;

import com.google.common.base.Objects;

import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.Encodable;

public abstract class ShardedMessage<T extends Encodable> extends EncodableMessage<T> {

    private final Identifier identifier;
    
    protected ShardedMessage(Identifier identifier, T message) {
        super(message);
        this.identifier = identifier;
    }
    
    public Identifier getIdentifier() {
        return identifier;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("identifier", getIdentifier())
                .add("message", delegate())
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
        ShardedMessage<?> other = (ShardedMessage<?>) obj;
        return Objects.equal(getIdentifier(), other.getIdentifier())
                && Objects.equal(delegate(), other.delegate());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getIdentifier(), delegate());
    }
}
