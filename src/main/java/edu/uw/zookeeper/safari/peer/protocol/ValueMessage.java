package edu.uw.zookeeper.safari.peer.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public abstract class ValueMessage<T,V> extends IdentifierMessage<T> {

    protected final V value;
    
    protected ValueMessage(T identifier, V value) {
        super(identifier);
        this.value = value;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("identifier", identifier)
                .add("value", value)
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
        ValueMessage<?,?> other = (ValueMessage<?,?>) obj;
        return Objects.equal(identifier, other.identifier)
                && Objects.equal(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identifier, value);
    }
}
