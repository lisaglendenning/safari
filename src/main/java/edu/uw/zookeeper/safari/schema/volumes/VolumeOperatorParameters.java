package edu.uw.zookeeper.safari.schema.volumes;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.UnsignedLong;

public abstract class VolumeOperatorParameters {

    private final UnsignedLong version;
    
    protected VolumeOperatorParameters(
            UnsignedLong version) {
        this.version = checkNotNull(version);
    }
    
    public final UnsignedLong getVersion() {
        return version;
    }
    
    @Override
    public String toString() {
        return toStringHelper(MoreObjects.toStringHelper(this)).toString();
    }
    
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper toString) {
        return toString.add("version", version);
    }
}
