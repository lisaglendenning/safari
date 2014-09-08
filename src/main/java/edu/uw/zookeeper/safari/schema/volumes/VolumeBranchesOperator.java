package edu.uw.zookeeper.safari.schema.volumes;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableBiMap;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public final class VolumeBranchesOperator {
    
    public static VolumeBranchesOperator create(
            VolumeDescriptor volume,
            ImmutableBiMap<ZNodeName, Identifier> branches) {
        return new VolumeBranchesOperator(volume, branches);
    }
    
    public static final class ParentAndChild<U,V> extends AbstractPair<U,V> {

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
    
    private final VolumeDescriptor volume;
    private final ImmutableBiMap<ZNodeName, Identifier> branches;
    
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
    
    public ImmutableBiMap<ZNodeName, Identifier> union(VolumeDescriptor child, ImmutableBiMap<ZNodeName, Identifier> grandchildren) {
        final ZNodeName prefix = branches.inverse().get(child.getId());
        checkArgument(prefix != null);
        ImmutableBiMap.Builder<ZNodeName, Identifier> union = ImmutableBiMap.builder();
        for (Map.Entry<ZNodeName, Identifier> branch: branches.entrySet()) {
            if (branch.getKey() != prefix) {
                union.put(branch);
            }
        }
        for (Map.Entry<ZNodeName, Identifier> grandchild: grandchildren.entrySet()) {
            union.put(RelativeZNodePath.fromString(
                            ZNodePath.join(
                                    prefix.toString(), 
                                    grandchild.getKey().toString())), 
                        grandchild.getValue());
        }
        return union.build();
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("volume", volume).add("branches", branches).toString();
    }
}
