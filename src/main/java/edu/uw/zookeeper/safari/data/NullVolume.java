package edu.uw.zookeeper.safari.data;

import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.common.Reference;

public final class NullVolume extends VersionedVolume {

    public static NullVolume getInstance() {
        return Holder.INSTANCE.instance;
    }

    private static enum Holder implements Reference<NullVolume> {
        INSTANCE(new NullVolume());
        
        private final NullVolume instance;
        
        private Holder(NullVolume instance) {
            this.instance = instance;
        }

        @Override
        public NullVolume get() {
            return instance;
        }
    }
    
    private NullVolume() {
        super(VolumeDescriptor.none(), UnsignedLong.ZERO);
    }
}
