package edu.uw.zookeeper.safari.storage.snapshot;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.safari.Identifier;

public final class SnapshotVolumeParameters {
    
    private final Identifier volume;
    private final ZNodeName branch;
    private final UnsignedLong version;
    
    public SnapshotVolumeParameters(Identifier volume, ZNodeName branch,
            UnsignedLong version) {
        this.volume = volume;
        this.branch = branch;
        this.version = version;
    }
    
    public Identifier getVolume() {
        return volume;
    }
    
    public ZNodeName getBranch() {
        return branch;
    }
    
    public UnsignedLong getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("volume", getVolume())
                .add("version", getVersion())
                .add("branch", getBranch()).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof SnapshotVolumeParameters)) {
            return false;
        }
        SnapshotVolumeParameters other = (SnapshotVolumeParameters) obj;
        return Objects.equal(getVolume(), other.getVolume())
                && Objects.equal(getVersion(), other.getVersion())
                && Objects.equal(getBranch(), other.getBranch());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getVolume());
    }
}