package edu.uw.zookeeper.safari.data;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.DefaultsNode;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.safari.Identifier;

public final class VolumeOverlayNode extends DefaultsNode.AbstractDefaultsNode<VolumeOverlayNode> {

    public static VolumeOverlayNode root(@Nullable StampedValue<Identifier> id) {
        return new VolumeOverlayNode(id, SimpleLabelTrie.<VolumeOverlayNode>rootPointer());
    }
    
    private StampedValue<Identifier> id;
    // we use these links as an overlay to speed-up traversal of the tree
    private final Map<ZNodeName, VolumeOverlayNode> overlay;

    public VolumeOverlayNode(
            Pointer<? extends VolumeOverlayNode> parent) {
        this(null, parent);
    }
    
    public VolumeOverlayNode(
            StampedValue<Identifier> id,
            Pointer<? extends VolumeOverlayNode> parent) {
        super(AbstractNameTrie.pathOf(parent), parent, Maps.<ZNodeName,VolumeOverlayNode>newHashMap());
        this.id = id;
        this.overlay = Maps.newHashMapWithExpectedSize(0);
    }

    public @Nullable StampedValue<Identifier> getId() {
        return id;
    }

    public void setId(@Nullable StampedValue<Identifier> id) {
        this.id = id;
    }
    
    public Map<ZNodeName, VolumeOverlayNode> getOverlay() {
        return overlay;
    }
    
    @Override
    protected VolumeOverlayNode newChild(ZNodeName name) {
        return new VolumeOverlayNode(AbstractNameTrie.weakPointer(name, this));
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("id", id)
                .toString();
    }
}