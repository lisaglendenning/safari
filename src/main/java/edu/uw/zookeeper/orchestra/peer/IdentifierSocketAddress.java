package edu.uw.zookeeper.orchestra.peer;

import java.net.SocketAddress;

import com.google.common.base.Objects;

import edu.uw.zookeeper.orchestra.Identifier;

public class IdentifierSocketAddress extends SocketAddress {

    public static IdentifierSocketAddress of(Identifier identifier, SocketAddress address) {
        return new IdentifierSocketAddress(identifier, address);
    }
    
    private static final long serialVersionUID = 6458941489539740979L;

    protected final Identifier identifier;
    protected final SocketAddress address;
    
    public IdentifierSocketAddress(Identifier identifier, SocketAddress address) {
        this.identifier = identifier;
        this.address = address;
    }
    
    public Identifier getIdentifier() {
        return identifier;
    }
    
    public SocketAddress getAddress() {
        return address;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("identifier", getIdentifier()).add("address", getAddress()).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof IdentifierSocketAddress)) {
            return false;
        }
        IdentifierSocketAddress other = (IdentifierSocketAddress) obj;
        return Objects.equal(getIdentifier(), other.getIdentifier())
                && Objects.equal(getAddress(), other.getAddress());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getIdentifier(), getAddress());
    }
}
