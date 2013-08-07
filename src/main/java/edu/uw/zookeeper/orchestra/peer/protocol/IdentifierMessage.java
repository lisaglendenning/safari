package edu.uw.zookeeper.orchestra.peer.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;

public abstract class IdentifierMessage<T> implements MessageBody {

    private final T identifier;
    
    protected IdentifierMessage(T identifier) {
        this.identifier = checkNotNull(identifier);
    }

    public T getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("identifier", getIdentifier())
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
        return Objects.equal(getIdentifier(), other.getIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getIdentifier());
    }
}
