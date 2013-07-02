package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.util.AbstractPair;

public class Volume extends AbstractPair<Identifier, VolumeDescriptor> {

    public static Volume of(Identifier first, VolumeDescriptor second) {
        return new Volume(first, second);
    }
    
    public Volume(Identifier first, VolumeDescriptor second) {
        super(first, second);
    }

    public Identifier getId() {
        return first;
    }
    
    public VolumeDescriptor getDescriptor() {
        return second;
    }
}
