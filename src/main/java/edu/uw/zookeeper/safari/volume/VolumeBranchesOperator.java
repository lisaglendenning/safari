package edu.uw.zookeeper.safari.volume;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableBiMap;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public class VolumeBranchesOperator {
    
    public static VolumeBranchesOperator create(
            VolumeDescriptor volume,
            ImmutableBiMap<ZNodeName, Identifier> branches) {
        return new VolumeBranchesOperator(volume, branches);
    }
    
    public static class ParentAndChild<U,V> extends AbstractPair<U,V> {

        public static <U,V> ParentAndChild<U,V> create(U parent, V child) {
            return new ParentAndChild<U,V>(parent, child);
        }
        protected ParentAndChild(U parent, V child) {
            super(parent, child);
        }
        
        public U getParent() {
            return first;
        }
        
        public V getChild() {
            return second;
        }
    }
    
    protected final VolumeDescriptor volume;
    protected final ImmutableBiMap<ZNodeName, Identifier> branches;
    
    protected VolumeBranchesOperator(
            VolumeDescriptor volume,
            ImmutableBiMap<ZNodeName, Identifier> branches) {
        this.volume = volume;
        this.branches = branches;
    }
    
    public VolumeDescriptor volume() {
        return volume;
    }
    
    public ImmutableBiMap<ZNodeName, Identifier> branches() {
        return branches;
    }

    public ParentAndChild<ImmutableBiMap<ZNodeName, Identifier>,ImmutableBiMap<ZNodeName, Identifier>> difference(VolumeDescriptor child) {
        checkArgument(VolumeBranches.contains(volume.getPath(), branches.keySet(), child.getPath()));
        final ZNodeName suffix = child.getPath().suffix(volume.getPath());
        checkArgument(! (suffix instanceof EmptyZNodeLabel));
        checkArgument(! branches.inverse().containsKey(child.getId()));
        ParentAndChild<ImmutableBiMap.Builder<ZNodeName, Identifier>,ImmutableBiMap.Builder<ZNodeName, Identifier>> difference = ParentAndChild.create(ImmutableBiMap.<ZNodeName, Identifier>builder(), ImmutableBiMap.<ZNodeName, Identifier>builder());
        difference.getParent().put(suffix, child.getId());
        for (Map.Entry<ZNodeName, Identifier> branch: branches.entrySet()) {
            if (branch.getKey().startsWith(suffix)) {
                difference.getChild().put(((RelativeZNodePath) branch.getKey()).suffix(suffix.length()), branch.getValue());
            } else {
                difference.getParent().put(branch.getKey(), branch.getValue());
            }
        }
        return ParentAndChild.create(difference.getParent().build(), difference.getChild().build());
    }
    
    public ImmutableBiMap<ZNodeName, Identifier> union(VolumeDescriptor parent, ImmutableBiMap<ZNodeName, Identifier> siblings) {
        checkArgument(siblings.values().contains(volume.getId()));
        ImmutableBiMap.Builder<ZNodeName, Identifier> union = ImmutableBiMap.builder();
        final ZNodeName prefix = siblings.inverse().get(volume.getId());
        for (Map.Entry<ZNodeName, Identifier> sibling: siblings.entrySet()) {
            if (sibling.getKey() != prefix) {
                union.put(sibling);
            }
        }
        for (Map.Entry<ZNodeName, Identifier> branch: branches.entrySet()) {
            union.put(
                    RelativeZNodePath.fromString(
                            ZNodePath.join(
                                    prefix.toString(), 
                                    branch.getKey().toString())), 
                    branch.getValue());
        }
        return union.build();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("volume", volume).add("branches", branches).toString();
    }
}
