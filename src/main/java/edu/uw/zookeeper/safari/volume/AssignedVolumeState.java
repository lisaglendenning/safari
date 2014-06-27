package edu.uw.zookeeper.safari.volume;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import edu.uw.zookeeper.safari.Identifier;

public abstract class AssignedVolumeState {
    
    private final Identifier region;
    
    protected AssignedVolumeState(
            Identifier region) {
        super();
        this.region = region;
    }

    public final Identifier getRegion() {
        return region;
    }

    public abstract ImmutableSet<Identifier> getLeaves();
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("region", getRegion()).add("leaves", getLeaves()).toString();
    }
}
