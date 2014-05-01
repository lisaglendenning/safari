package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLong;

public abstract class VersionedVolume {

    private final VolumeDescriptor descriptor;
    private final UnsignedLong version;
    
    protected VersionedVolume(
            VolumeDescriptor descriptor, 
            UnsignedLong version) {
        this.descriptor = checkNotNull(descriptor);
        this.version = checkNotNull(version);
    }

    public final VolumeDescriptor getDescriptor() {
        return descriptor;
    }
    
    public final UnsignedLong getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("descriptor", getDescriptor())
                .add("version", getVersion()).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof EmptyVolume)) {
            return false;
        }
        EmptyVolume other = (EmptyVolume) obj;
        return getDescriptor().equals(other.getDescriptor())
                && getDescriptor().equals(other.getDescriptor());
    }
    
    @Override
    public int hashCode() {
        return descriptor.getId().hashCode();
    }
}
