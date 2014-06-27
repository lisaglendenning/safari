package edu.uw.zookeeper.safari.volume;

import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedValue;

public class EmptyVolume extends VolumeVersion<Void> {

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
    
    protected EmptyVolume(
            VolumeDescriptor descriptor, 
            UnsignedLong version) {
        super(descriptor, VersionedValue.<Void>valueOf(version, null));
    }
}
