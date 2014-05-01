package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.safari.Identifier;

public final class VolumeState {

    public static VolumeState valueOf(
            Identifier region,
            Collection<Identifier> leaves) {
        return new VolumeState(region, leaves);
    }
    
    public static VolumeState none() {
        return Holder.NONE.get();
    }

    protected static enum Holder implements Reference<VolumeState> {
        NONE(new VolumeState(Identifier.zero(), ImmutableSet.<Identifier>of()));
        
        private final VolumeState instance;
        
        private Holder(VolumeState instance) {
            this.instance = instance;
        }

        @Override
        public VolumeState get() {
            return instance;
        }
    }
    
    private final Identifier region;
    private final ImmutableSet<Identifier> leaves;
    
    @JsonCreator
    public VolumeState(
            @JsonProperty("region") Identifier region,
            @JsonProperty("leaves") Collection<Identifier> leaves) {
        super();
        this.region = checkNotNull(region);
        this.leaves = ImmutableSet.copyOf(leaves);
    }

    public Identifier getRegion() {
        return region;
    }

    public ImmutableSet<Identifier> getLeaves() {
        return leaves;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("region", region).add("leaves", leaves).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof VolumeState)) {
            return false;
        }
        VolumeState other = (VolumeState) obj;
        return region.equals(other.region)
                && leaves.equals(other.leaves);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(region, leaves);
    }
}
