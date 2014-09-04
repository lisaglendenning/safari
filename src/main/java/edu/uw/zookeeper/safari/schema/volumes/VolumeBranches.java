package edu.uw.zookeeper.safari.schema.volumes;

import static com.google.common.base.Preconditions.*;

import java.util.Set;

import com.google.common.collect.ImmutableBiMap;

import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedValue;

public final class VolumeBranches extends VolumeVersion<ImmutableBiMap<ZNodeName, Identifier>> {

    public static VolumeBranches valueOf(
            VolumeDescriptor descriptor, 
            VersionedValue<ImmutableBiMap<ZNodeName, Identifier>> state) {
        return new VolumeBranches(descriptor, state);
    }

    public static boolean contains(final VolumeBranches volume, final ZNodePath path) {
        return contains(volume.getDescriptor().getPath(), volume.getState().getValue().keySet(), path);
    }
    
    public static boolean contains(
            final ZNodePath root, final Set<ZNodeName> branches, final ZNodePath path) {
        return (path.startsWith(root) && contains(branches, path.suffix(root)));
    }
    
    public static boolean contains(final Set<ZNodeName> branches, final ZNodeName remaining) {
        if (! (remaining instanceof EmptyZNodeLabel)) {
            for (ZNodeName branch: branches) {
                if (branch.startsWith(remaining)) {
                    return false;
                }
            }
        }
        return true;
    }

    protected VolumeBranches(
            VolumeDescriptor descriptor, 
            VersionedValue<ImmutableBiMap<ZNodeName, Identifier>> state) {
        super(descriptor, checkNotNull(state));
    }
}
