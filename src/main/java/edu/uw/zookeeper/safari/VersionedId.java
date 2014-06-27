package edu.uw.zookeeper.safari;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.UnsignedLong;

public final class VersionedId extends VersionedValue<Identifier> implements Comparable<VersionedId> {

    public static VersionedId valueOf(UnsignedLong version, Identifier identifier) {
        return new VersionedId(version, identifier);
    }

    public static VersionedId zero() {
        return valueOf(UnsignedLong.ZERO, Identifier.zero());
    }
    
    @JsonCreator
    public VersionedId(
            @JsonProperty("version") UnsignedLong version,
            @JsonProperty("value") Identifier value) {
        super(version, value);
    }

    @Override
    public int compareTo(VersionedId other) {
        int ret = getValue().compareTo(other.getValue());
        if (ret != 0) {
            return ret;
        }
        return getVersion().compareTo(other.getVersion());
    }
}
