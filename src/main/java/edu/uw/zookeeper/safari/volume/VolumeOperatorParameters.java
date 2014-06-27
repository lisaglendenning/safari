package edu.uw.zookeeper.safari.volume;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLong;

public abstract class VolumeOperatorParameters {

    private final UnsignedLong version;
    
    protected VolumeOperatorParameters(
            UnsignedLong version) {
        this.version = version;
    }
    
    public final UnsignedLong getVersion() {
        return version;
    }
    
    @Override
    public String toString() {
        return toStringHelper(Objects.toStringHelper(this)).toString();
    }
    
    protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper toString) {
        return toString.add("version", version);
    }
}
