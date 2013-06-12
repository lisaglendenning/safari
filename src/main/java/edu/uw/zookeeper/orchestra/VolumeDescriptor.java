package edu.uw.zookeeper.orchestra;

import java.util.SortedSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedSet;

import edu.uw.zookeeper.data.ZNodeLabel;

public class VolumeDescriptor {
    
    public static VolumeDescriptor of(ZNodeLabel.Path root) {
        return of(root, ImmutableSortedSet.<ZNodeLabel>of());
    }

    public static VolumeDescriptor of(
            ZNodeLabel.Path root, 
            SortedSet<ZNodeLabel> leaves) {
        return new VolumeDescriptor(root, leaves);
    }
    
    protected final ZNodeLabel.Path root;
    protected final SortedSet<ZNodeLabel> leaves;

    @JsonCreator
    public VolumeDescriptor(
            @JsonProperty("root") ZNodeLabel.Path root, 
            @JsonProperty("leaves") SortedSet<ZNodeLabel> leaves) {
        this.root = root;
        this.leaves = ImmutableSortedSet.copyOf(leaves);
    }
    
    public ZNodeLabel.Path getRoot() {
        return root;
    }
    
    public SortedSet<ZNodeLabel> getLeaves() {
        return leaves;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("root", getRoot())
                .add("leaves", getLeaves()).toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getRoot(), getLeaves());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        VolumeDescriptor other = (VolumeDescriptor) obj;
        return Objects.equal(getRoot(), other.getRoot()) 
                && Objects.equal(getLeaves(), other.getLeaves());
    }
}
