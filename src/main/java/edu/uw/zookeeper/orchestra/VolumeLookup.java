package edu.uw.zookeeper.orchestra;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;

public class VolumeLookup {

    public static VolumeLookup newInstance() {
        return new VolumeLookup(
                ZNodeLabelTrie.of(VolumeLookupNode.root()), 
                new MapMaker().<Identifier, ZNodeLabel.Path>makeMap());
    }
    
    protected final ZNodeLabelTrie<VolumeLookupNode> lookupTrie;
    protected final ConcurrentMap<Identifier, ZNodeLabel.Path> byVolumeId;
    
    protected VolumeLookup(
            ZNodeLabelTrie<VolumeLookupNode> lookupTrie,
            ConcurrentMap<Identifier, ZNodeLabel.Path> byVolumeId) {
        this.lookupTrie = lookupTrie;
        this.byVolumeId = byVolumeId;
    }
    
    public void clear() {
        lookupTrie.clear();
        byVolumeId.clear();
    }

    public ZNodeLabelTrie<VolumeLookupNode> asTrie() {
        return lookupTrie;
    }

    public VolumeAssignment get(ZNodeLabel label) {
        VolumeLookupNode node = asTrie().longestPrefix(label);
        VolumeAssignment assignment = node.get();
        while ((assignment == null) && node.parent().isPresent()) {
            node = node.parent().orNull().get();
            assignment = node.get();
        }
        return assignment;
    }

    public VolumeAssignment byVolumeId(Identifier id) {
        ZNodeLabel.Path path = getVolumeRoot(id);
        if (path != null) {
            VolumeLookupNode node = asTrie().get(path);
            if (node != null) {
                return node.get();
            }
        }
        return null;
    }
    
    public Set<Identifier> getVolumeIds() {
        return byVolumeId.keySet();
    }
    
    public ZNodeLabel.Path getVolumeRoot(Identifier id) {
        return byVolumeId.get(id);
    }

    public ZNodeLabel.Path putVolumeRoot(Identifier id, ZNodeLabel.Path root) {
        return byVolumeId.put(id, root);
    }

    public ZNodeLabel.Path removeVolumeRoot(Identifier id) {
        return byVolumeId.remove(id);
    }
    
    protected static class VolumeLookupNode extends ZNodeLabelTrie.DefaultsNode<VolumeLookupNode> {
    
        public static VolumeLookupNode root() {
            return new VolumeLookupNode(
                    Optional.<ZNodeLabelTrie.Pointer<VolumeLookupNode>>absent());
        }
        
        protected AtomicReference<VolumeAssignment> value;
    
        protected VolumeLookupNode(
                Optional<ZNodeLabelTrie.Pointer<VolumeLookupNode>> parent) {
            this(null, parent);
        }
        
        protected VolumeLookupNode(
                VolumeAssignment value,
                Optional<ZNodeLabelTrie.Pointer<VolumeLookupNode>> parent) {
            super(parent);
            this.value = new AtomicReference<VolumeAssignment>(value);
        }
        
        public VolumeAssignment get() {
            return value.get();
        }
        
        public VolumeAssignment getAndSet(VolumeAssignment value) {
            return this.value.getAndSet(value);
        }
        
        public VolumeAssignment setVolume(Volume volume) {
            VolumeAssignment prev = value.get();
            VolumeAssignment updated = (prev == null) 
                    ? VolumeAssignment.of(volume, null)
                            : prev.setVolume(volume);
            if (value.compareAndSet(prev, updated)) {
                return prev;
            } else {
                return setVolume(volume);
            }
        }
        
        public VolumeAssignment setAssignment(Identifier assignment) {
            VolumeAssignment prev = value.get();
            VolumeAssignment updated = (prev == null) 
                    ? VolumeAssignment.of(Volume.of(null, VolumeDescriptor.of(path())), assignment)
                            : prev.setAssignment(assignment);
            if (value.compareAndSet(prev, updated)) {
                return prev;
            } else {
                return setAssignment(assignment);
            }
        }
    
        protected VolumeLookupNode newChild(ZNodeLabel.Component label) {
            ZNodeLabelTrie.Pointer<VolumeLookupNode> pointer = ZNodeLabelTrie.SimplePointer.of(label, this);
            return new VolumeLookupNode(Optional.of(pointer));
        }
    }
    
}
