package edu.uw.zookeeper.safari.volume;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.safari.VersionedId;

public class MergeParameters extends VolumeOperatorParameters {

    public static MergeParameters create(
            VersionedId parent,
            UnsignedLong version) {
        return new MergeParameters(parent, version);
    }
    
    private final VersionedId parent;

    @JsonCreator
    public MergeParameters(
            @JsonProperty("parent") VersionedId parent,
            @JsonProperty("version") UnsignedLong version) {
        super(version);
        this.parent = parent;
    }

    public final VersionedId getParent() {
        return parent;
    }

    @Override
    protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper toString) {
        return super.toStringHelper(toString).add("parent", parent);
    }
}
