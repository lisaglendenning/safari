package edu.uw.zookeeper.safari.data;

import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public final class EmptyVolume extends VersionedVolume {

    public static EmptyVolume valueOf(
            Identifier id,
            ZNodePath path,
            UnsignedLong version) {
        return valueOf(VolumeDescriptor.valueOf(id, path), version);
    }
    
    public static EmptyVolume valueOf(
            VolumeDescriptor descriptor, 
            UnsignedLong version) {
        return new EmptyVolume(descriptor, version);
    }
    
    public EmptyVolume(
            VolumeDescriptor descriptor, 
            UnsignedLong version) {
        super(descriptor, version);
    }
}
