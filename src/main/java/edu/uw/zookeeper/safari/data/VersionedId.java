package edu.uw.zookeeper.safari.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.safari.Identifier;

public final class VersionedId extends AbstractPair<Identifier, UnsignedLong> implements Comparable<VersionedId> {

    public static VersionedId valueOf(Identifier identifier, UnsignedLong version) {
        return new VersionedId(identifier, version);
    }

    public static VersionedId zero() {
        return valueOf(Identifier.zero(), UnsignedLong.ZERO);
    }
    
    @JsonCreator
    public VersionedId(@JsonProperty("identifier") Identifier identifier, 
            @JsonProperty("version") UnsignedLong version) {
        super(identifier, version);
    }
    
    public Identifier getIdentifier() {
        return first;
    }
    
    public UnsignedLong getVersion() {
        return second;
    }

    @Override
    public int compareTo(VersionedId other) {
        int ret = first.compareTo(other.first);
        if (ret != 0) {
            return ret;
        }
        return second.compareTo(other.second);
    }
}
