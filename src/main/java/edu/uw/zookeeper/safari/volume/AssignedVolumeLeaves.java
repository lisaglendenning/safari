package edu.uw.zookeeper.safari.volume;

import edu.uw.zookeeper.safari.VersionedValue;

public final class AssignedVolumeLeaves extends VolumeVersion<RegionAndLeaves> {

    public static AssignedVolumeLeaves valueOf(
            VolumeDescriptor descriptor, 
            VersionedValue<RegionAndLeaves> state) {
        return new AssignedVolumeLeaves(descriptor, state);
    }
    
    protected AssignedVolumeLeaves(
            VolumeDescriptor descriptor,
            VersionedValue<RegionAndLeaves> state) {
        super(descriptor, state);
    }
}
