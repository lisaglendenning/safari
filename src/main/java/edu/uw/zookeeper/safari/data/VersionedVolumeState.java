package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public final class VersionedVolumeState extends VersionedVolume {

    public static VersionedVolumeState valueOf(
            Identifier id,
            ZNodePath path,
            UnsignedLong version,
            VolumeState state) {
        return valueOf(VolumeDescriptor.valueOf(id, path), version, state);
    }
    
    public static VersionedVolumeState valueOf(
            VolumeDescriptor descriptor, 
            UnsignedLong version,
            VolumeState state) {
        return new VersionedVolumeState(descriptor, version, state);
    }
    
    private final VolumeState state;
    
    public VersionedVolumeState(
            VolumeDescriptor descriptor, 
            UnsignedLong version,
            VolumeState state) {
        super(descriptor, version);
        this.state = checkNotNull(state);
    }

    public VolumeState getState() {
        return state;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("descriptor", getDescriptor())
                .add("version", getVersion())
                .add("state", state).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof VersionedVolumeState)) {
            return false;
        }
        VersionedVolumeState other = (VersionedVolumeState) obj;
        return getDescriptor().equals(other.getDescriptor())
                && getDescriptor().equals(other.getDescriptor())
                && state.equals(other.state);
    }
}
