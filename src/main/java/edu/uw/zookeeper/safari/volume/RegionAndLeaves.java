package edu.uw.zookeeper.safari.volume;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.safari.Identifier;

public final class RegionAndLeaves extends AssignedVolumeState {

    public static RegionAndLeaves copyOf(
            AssignedVolumeState state) {
        if (state instanceof RegionAndLeaves) {
            return (RegionAndLeaves) state;
        } else {
            return valueOf(state.getRegion(), state.getLeaves());
        }
    }

    public static RegionAndLeaves empty(
            Identifier region) {
        return valueOf(region, ImmutableSet.<Identifier>of());
    }
    
    public static RegionAndLeaves valueOf(
            Identifier region,
            Collection<Identifier> leaves) {
        return new RegionAndLeaves(region, leaves);
    }
    
    public static RegionAndLeaves none() {
        return Holder.NONE.get();
    }

    protected static enum Holder implements Reference<RegionAndLeaves> {
        NONE(new RegionAndLeaves(Identifier.zero(), ImmutableSet.<Identifier>of()));
        
        private final RegionAndLeaves instance;
        
        private Holder(RegionAndLeaves instance) {
            this.instance = instance;
        }

        @Override
        public RegionAndLeaves get() {
            return instance;
        }
    }
    
    private final ImmutableSet<Identifier> leaves;
    
    @JsonCreator
    public RegionAndLeaves(
            @JsonProperty("region") Identifier region,
            @JsonProperty("leaves") Collection<Identifier> leaves) {
        super(checkNotNull(region));
        this.leaves = ImmutableSet.copyOf(leaves);
    }

    @Override
    public final ImmutableSet<Identifier> getLeaves() {
        return leaves;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof RegionAndLeaves)) {
            return false;
        }
        RegionAndLeaves other = (RegionAndLeaves) obj;
        return getRegion().equals(other.getRegion())
                && getLeaves().equals(other.getLeaves());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getRegion(), getLeaves());
    }
}
