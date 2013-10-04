package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

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
        return of(root, ImmutableSet.<ZNodeLabel>of());
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
    
    protected static class Canonicalize implements Function<ZNodeLabel, ZNodeLabel> {
        
        public static Canonicalize of(ZNodeLabel.Path root) {
            return new Canonicalize(root);
        }
        
        private final ZNodeLabel.Path root;
        
        public Canonicalize(ZNodeLabel.Path root) {
            this.root = root;
        }

        @Override
        public ZNodeLabel apply(ZNodeLabel input) {
            ZNodeLabel output = input;
            if (input instanceof ZNodeLabel.Path) {
                ZNodeLabel.Path path = (ZNodeLabel.Path) input;
                if (path.isAbsolute()) {
                    checkArgument(root.prefixOf(path), input);
                    int rootLength = root.toString().length();
                    if (rootLength == 1) {
                        return path.suffix(0);
                    } else {
                        return path.suffix(rootLength + 1);
                    }
                }
            }
            return output;
        }
    }

    @JsonCreator
    public VolumeDescriptor(
            @JsonProperty("root") ZNodeLabel.Path root, 
            @JsonProperty("leaves") Collection<ZNodeLabel> leaves) {
        super(root, ImmutableSortedSet.copyOf(Iterables.transform(leaves, Canonicalize.of(root))));
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
    
    public VolumeDescriptor add(ZNodeLabel leaf) {
        ImmutableSortedSet<ZNodeLabel> leaves = 
                ImmutableSortedSet.<ZNodeLabel>naturalOrder()
                .addAll(getLeaves()).add(leaf).build();
        return VolumeDescriptor.of(getRoot(), leaves);
    }
    
    public VolumeDescriptor remove(ZNodeLabel leaf) {
        Sets.SetView<ZNodeLabel> leaves = Sets.difference(getLeaves(), ImmutableSet.of(leaf));
        return VolumeDescriptor.of(getRoot(), leaves);
    }
}
