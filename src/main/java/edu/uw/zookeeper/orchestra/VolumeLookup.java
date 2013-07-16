package edu.uw.zookeeper.orchestra;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.util.Reference;

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
    
    public Volume put(Volume volume) {
        ZNodeLabel.Path volumeRoot = volume.getDescriptor().getRoot();
        byVolumeId.put(volume.getId(), volumeRoot);
        VolumeLookupNode node = asTrie().root().add(volumeRoot);
        return node.set(volume);
    }

    public Volume remove(Identifier id) {
        Volume prev = null;
        ZNodeLabel.Path volumeRoot = byVolumeId.remove(id);
        if (volumeRoot != null) {
            VolumeLookup.VolumeLookupNode lookupNode = asTrie().get(volumeRoot);
            if (lookupNode != null) {
                prev = lookupNode.getAndSet(null);
            }
        }
        return prev;
    }

    public Volume get(ZNodeLabel.Path path) {
        VolumeLookupNode node = asTrie().longestPrefix(path);
        return (node == null) ? null : node.get();
    }
    
    public Volume get(Identifier id) {
        ZNodeLabel.Path path = byVolumeId.get(id);
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
    
    protected static class VolumeLookupNode extends ZNodeLabelTrie.DefaultsNode<VolumeLookupNode> implements Reference<Volume> {
    
        public static VolumeLookupNode root() {
            return new VolumeLookupNode(
                    Optional.<ZNodeLabelTrie.Pointer<VolumeLookupNode>>absent());
        }
        
        protected AtomicReference<Volume> value;
    
        protected VolumeLookupNode(
                Optional<ZNodeLabelTrie.Pointer<VolumeLookupNode>> parent) {
            this(null, parent);
        }
        
        protected VolumeLookupNode(
                Volume value,
                Optional<ZNodeLabelTrie.Pointer<VolumeLookupNode>> parent) {
            super(parent);
            this.value = new AtomicReference<Volume>(value);
        }
        
        @Override
        public Volume get() {
            return value.get();
        }
        
        public Volume getAndSet(Volume value) {
            return this.value.getAndSet(value);
        }
        
        public Volume set(Volume volume) {
            Volume prev = value.get();
            if (value.compareAndSet(prev, volume)) {
                return prev;
            } else {
                return set(volume);
            }
        }

        protected VolumeLookupNode newChild(ZNodeLabel.Component label) {
            ZNodeLabelTrie.Pointer<VolumeLookupNode> pointer = ZNodeLabelTrie.SimplePointer.of(label, this);
            return new VolumeLookupNode(Optional.of(pointer));
        }
    }
    
}
