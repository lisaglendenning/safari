package edu.uw.zookeeper.orchestra.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.orchestra.Identifier;

public class Volume extends AbstractPair<Identifier, VolumeDescriptor> {

    public static Volume none() {
        return Holder.NONE.get();
    }
    
    public static Volume of(Identifier id, VolumeDescriptor descriptor) {
        return new Volume(id, descriptor);
    }

    protected static enum Holder implements Reference<Volume> {
        NONE(new Volume(Identifier.zero(), VolumeDescriptor.none()));
        
        private final Volume instance;
        
        private Holder(Volume instance) {
            this.instance = instance;
        }

        @Override
        public Volume get() {
            return instance;
        }
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
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(getId()).addValue(getDescriptor()).toString();
    }
}
