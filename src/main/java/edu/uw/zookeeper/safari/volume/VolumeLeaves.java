package edu.uw.zookeeper.safari.volume;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.ImmutableSet;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedValue;

public final class VolumeLeaves extends VolumeVersion<ImmutableSet<Identifier>> {

    public static VolumeLeaves valueOf(
            Identifier id, 
            ZNodePath path, 
            VersionedValue<ImmutableSet<Identifier>> state) {
        return valueOf(VolumeDescriptor.valueOf(id, path), state);
    }

    public static VolumeLeaves valueOf(
            VolumeDescriptor descriptor, 
            VersionedValue<ImmutableSet<Identifier>> state) {
        return new VolumeLeaves(descriptor, state);
    }

    protected VolumeLeaves(
            VolumeDescriptor descriptor, 
            VersionedValue<ImmutableSet<Identifier>> state) {
        super(descriptor, checkNotNull(state));
    }
}
