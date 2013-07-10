package edu.uw.zookeeper.orchestra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.util.AbstractPair;

public class Volume extends AbstractPair<Identifier, VolumeDescriptor> {

    public static Volume of(Identifier id, VolumeDescriptor descriptor) {
        return new Volume(id, descriptor);
    }

    @JsonCreator
    public Volume(
            @JsonProperty("id") Identifier id, 
            @JsonProperty("descriptor") VolumeDescriptor descriptor) {
        super(id, descriptor);
    }

    public Identifier getId() {
        return first;
    }
    
    public VolumeDescriptor getDescriptor() {
        return second;
    }
}
