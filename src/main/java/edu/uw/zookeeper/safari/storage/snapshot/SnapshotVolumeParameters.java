package edu.uw.zookeeper.safari.storage.snapshot;

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
    
    // TODO equals toString
    
}