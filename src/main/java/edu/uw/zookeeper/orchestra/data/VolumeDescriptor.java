package edu.uw.zookeeper.orchestra.data;

import java.util.Collection;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSortedSet;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.ZNodeLabel;

public class VolumeDescriptor extends AbstractPair<ZNodeLabel.Path, ImmutableSortedSet<ZNodeLabel>> {
    
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

    @JsonCreator
    public VolumeDescriptor(
            @JsonProperty("root") ZNodeLabel.Path root, 
            @JsonProperty("leaves") Collection<ZNodeLabel> leaves) {
        super(root, ImmutableSortedSet.copyOf(leaves));
    }
    
    public ZNodeLabel.Path getRoot() {
        return first;
    }
    
    public ImmutableSortedSet<ZNodeLabel> getLeaves() {
        return second;
    }
    
    public ZNodeLabel.Path append(Object label) {
        return (ZNodeLabel.Path) ZNodeLabel.joined(getRoot(), label);
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
}
