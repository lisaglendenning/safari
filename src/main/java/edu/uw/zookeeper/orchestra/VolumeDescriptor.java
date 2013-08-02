package edu.uw.zookeeper.orchestra;

import java.util.Collection;
import java.util.SortedSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSortedSet;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.ZNodeLabel;

public class VolumeDescriptor {
    
    public static VolumeDescriptor all() {
        return Holder.ALL.get();
    }

    public static VolumeDescriptor none() {
        return Holder.NONE.get();
    }
    
    public static VolumeDescriptor of(ZNodeLabel.Path root) {
        return of(root, ImmutableSortedSet.<ZNodeLabel>of());
    }

    public static VolumeDescriptor of(
            ZNodeLabel.Path root, 
            Collection<ZNodeLabel> leaves) {
        return new VolumeDescriptor(root, leaves);
    }

    protected static enum Holder implements Reference<VolumeDescriptor> {
        NONE(new VolumeDescriptor(ZNodeLabel.Path.root(), ImmutableSortedSet.<ZNodeLabel>of(ZNodeLabel.Path.root()))),
        ALL(new VolumeDescriptor(ZNodeLabel.Path.root(), ImmutableSortedSet.<ZNodeLabel>of()));
        
        private final VolumeDescriptor instance;
        
        private Holder(VolumeDescriptor instance) {
            this.instance = instance;
        }

        @Override
        public VolumeDescriptor get() {
            return instance;
        }
    }
    
    protected final ZNodeLabel.Path root;
    protected final ImmutableSortedSet<ZNodeLabel> leaves;

    @JsonCreator
    public VolumeDescriptor(
            @JsonProperty("root") ZNodeLabel.Path root, 
            @JsonProperty("leaves") Collection<ZNodeLabel> leaves) {
        this.root = root;
        this.leaves = ImmutableSortedSet.copyOf(leaves);
    }
    
    public ZNodeLabel.Path getRoot() {
        return root;
    }
    
    public SortedSet<ZNodeLabel> getLeaves() {
        return leaves;
    }
    
    public ZNodeLabel.Path append(ZNodeLabel label) {
        return ZNodeLabel.Path.of(getRoot(), label);
    }
    
    public boolean contains(ZNodeLabel.Path path) {
        if (! getRoot().prefixOf(path)) {
            return false;
        }
        for (ZNodeLabel leaf: getLeaves()) {
            if (append(leaf).prefixOf(path)) {
                return false;
            }
        }
        return true;
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
