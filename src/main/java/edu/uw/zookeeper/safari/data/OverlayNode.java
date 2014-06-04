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

/**
 * Not thread-safe.
 */
public final class OverlayNode<V> extends DefaultsNode.AbstractDefaultsNode<OverlayNode<V>> {

    public static <V> OverlayNode<V> root(@Nullable StampedValue<V> value) {
        return new OverlayNode<V>(value, SimpleLabelTrie.<OverlayNode<V>>rootPointer());
    }
    
    private StampedValue<V> value;
    // we use these links as an overlay to speed-up traversal of the tree
    private final Map<ZNodeName, OverlayNode<V>> overlay;

    public OverlayNode(
            Pointer<? extends OverlayNode<V>> parent) {
        this(null, parent);
    }
    
    public OverlayNode(
            StampedValue<V> value,
            Pointer<? extends OverlayNode<V>> parent) {
        super(AbstractNameTrie.pathOf(parent), parent, Maps.<ZNodeName,OverlayNode<V>>newHashMap());
        this.value = value;
        this.overlay = Maps.newHashMapWithExpectedSize(0);
    }

    public @Nullable StampedValue<V> getValue() {
        return value;
    }

    public void setValue(@Nullable StampedValue<V> value) {
        this.value = value;
    }
    
    public Map<ZNodeName, OverlayNode<V>> getOverlay() {
        return overlay;
    }
    
    @Override
    protected OverlayNode<V> newChild(ZNodeName name) {
        return new OverlayNode<V>(AbstractNameTrie.weakPointer(name, this));
    }

    @Override
    public String toString() {
        return Objects.toStringHelper("")
                .add("path", path())
                .add("children", keySet())
                .add("value", value)
                .add("overlay", overlay)
                .toString();
    }
}