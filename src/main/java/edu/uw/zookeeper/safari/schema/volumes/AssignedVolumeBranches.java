package edu.uw.zookeeper.safari.schema.volumes;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.VersionedValue;

public final class AssignedVolumeBranches extends VolumeVersion<RegionAndBranches> {

    public static AssignedVolumeBranches valueOf(
            VolumeDescriptor descriptor, 
            VersionedValue<RegionAndBranches> state) {
        return new AssignedVolumeBranches(descriptor, state);
    }

    public static boolean contains(final AssignedVolumeBranches volume, final ZNodePath path) {
        return VolumeBranches.contains(volume.getDescriptor().getPath(), volume.getState().getValue().getBranches().keySet(), path);
    }
    
    protected AssignedVolumeBranches(
            VolumeDescriptor descriptor, 
            VersionedValue<RegionAndBranches> state) {
        super(descriptor, state);
    }
}
