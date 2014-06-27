package edu.uw.zookeeper.safari.volume;

import static com.google.common.base.Preconditions.*;

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.safari.Identifier;

public final class RegionAndBranches extends AssignedVolumeState {

    public static RegionAndBranches empty(
            Identifier region) {
        return valueOf(region, ImmutableMap.<ZNodeName, Identifier>of());
    }
    
    public static RegionAndBranches valueOf(
            Identifier region,
            Map<ZNodeName, Identifier> branches) {
        return new RegionAndBranches(region, branches);
    }
    
    private final ImmutableBiMap<ZNodeName, Identifier> branches;
    
    public RegionAndBranches(
            Identifier region,
            Map<ZNodeName, Identifier> branches) {
        super(checkNotNull(region));
        this.branches = ImmutableBiMap.copyOf(branches);
    }

    @Override
    public final ImmutableSet<Identifier> getLeaves() {
        return getBranches().values();
    }
    
    public ImmutableBiMap<ZNodeName, Identifier> getBranches() {
        return branches;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("region", getRegion()).add("branches", getBranches()).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof RegionAndBranches)) {
            return false;
        }
        RegionAndBranches other = (RegionAndBranches) obj;
        return getRegion().equals(other.getRegion())
                && getBranches().equals(other.getBranches());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getRegion(), getBranches());
    }
}
