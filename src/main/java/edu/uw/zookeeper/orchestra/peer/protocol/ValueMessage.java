package edu.uw.zookeeper.orchestra.peer.protocol;

import com.google.common.base.Objects;

public abstract class ValueMessage<T,V> extends IdentifierMessage<T> {

    private final V value;
    
    protected ValueMessage(T identifier, V value) {
        super(identifier);
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("identifier", getIdentifier())
                .add("value", getValue())
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
        return Objects.equal(getIdentifier(), other.getIdentifier())
                && Objects.equal(getValue(), other.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getIdentifier(), getValue());
    }
}
