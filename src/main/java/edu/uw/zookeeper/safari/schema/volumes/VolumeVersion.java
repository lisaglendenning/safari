package edu.uw.zookeeper.safari.schema.volumes;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.safari.VersionedValue;

public abstract class VolumeVersion<V> extends AbstractPair<VolumeDescriptor, VersionedValue<V>> {

    protected VolumeVersion(
            VolumeDescriptor descriptor, 
            VersionedValue<V> state) {
        super(descriptor, state);
    }

    public final VolumeDescriptor getDescriptor() {
        return first;
    }
    
    public final VersionedValue<V> getState() {
        return second;
    }
}
