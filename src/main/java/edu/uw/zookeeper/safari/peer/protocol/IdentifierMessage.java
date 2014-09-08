package edu.uw.zookeeper.safari.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public abstract class IdentifierMessage<T> implements MessageBody {

    protected final T identifier;
    
    protected IdentifierMessage(T identifier) {
        this.identifier = checkNotNull(identifier);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("identifier", identifier)
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
        IdentifierMessage<?> other = (IdentifierMessage<?>) obj;
        return Objects.equal(identifier, other.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identifier);
    }
}
