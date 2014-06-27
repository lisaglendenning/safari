package edu.uw.zookeeper.safari.volume;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableBiMap;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.safari.Identifier;

public class AssignedVolumeOperator {

    public static AssignedVolumeOperator create(
            Identifier region,
            VolumeBranchesOperator operator) {
        return new AssignedVolumeOperator(region, operator);
    }
    
    protected final Identifier region;
    protected final VolumeBranchesOperator operator;
    
    protected AssignedVolumeOperator(
            Identifier region,
            VolumeBranchesOperator operator) {
        this.region = region;
        this.operator = operator;
    }
    
    public Identifier region() {
        return region;
    }
    
    public VolumeBranchesOperator operator() {
        return operator;
    }
    
    public RegionAndBranches assign(Identifier region) {
        return RegionAndBranches.valueOf(region, operator().branches());
    }
    
    public VolumeBranchesOperator.ParentAndChild<RegionAndBranches,RegionAndBranches> difference(VolumeDescriptor child) {
        VolumeBranchesOperator.ParentAndChild<ImmutableBiMap<ZNodeName, Identifier>,ImmutableBiMap<ZNodeName, Identifier>> difference = operator().difference(child);
        return VolumeBranchesOperator.ParentAndChild.create(RegionAndBranches.valueOf(region(), difference.getParent()), RegionAndBranches.valueOf(region(), difference.getChild()));
    }
    
    public RegionAndBranches union(VolumeDescriptor parent, ImmutableBiMap<ZNodeName, Identifier> siblings) {
        return RegionAndBranches.valueOf(region(), operator().union(parent, siblings));
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("region", region()).add("operator", operator()).toString();
    }
}
