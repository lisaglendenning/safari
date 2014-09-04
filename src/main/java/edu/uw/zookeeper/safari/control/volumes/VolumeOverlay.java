package edu.uw.zookeeper.safari.control.volumes;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.sun.xml.internal.xsom.impl.scd.Iterators;

import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

/**
 * Not thread-safe
 */
public final class VolumeOverlay implements Function<ZNodePath, StampedValue<Identifier>> {
    
    public static VolumeOverlay empty() {
        return new VolumeOverlay(SimpleLabelTrie.forRoot(OverlayNode.<Identifier>root(null)));
    }

    private final NameTrie<OverlayNode<Identifier>> trie;
    
    protected VolumeOverlay(
            NameTrie<OverlayNode<Identifier>> trie) {
        this.trie = trie;
    }
    
    public @Nullable StampedValue<Identifier> apply(final ZNodePath path) {
        OverlayNode<Identifier> node = get(path);
        return (node == null) ? null : node.getValue();
    }

    public boolean add(final long stamp, final ZNodePath path, final Identifier id) {
        OverlayNode<Identifier> node = get(path);
        OverlayNode<Identifier> parent = null;
        ZNodeName remaining = null;
        if (node != null) {
            if (node.path().length() == path.length()) {
                // my path
                if (node.getValue().stamp() >= stamp) {
                    // don't overwrite more up-to-date information
                    checkArgument((node.getValue().stamp() > stamp) || id.equals(node.getValue().get()));
                    return false;
                }
            } else {
                // parent
                parent = node;
                // FIXME ??
//                if (parent.getId().stamp() > stamp) {
//                    return false;
//                }
                remaining = path.suffix(parent.path());
                Iterator<ZNodeLabel> labels = (remaining instanceof ZNodeLabelVector) ? ((ZNodeLabelVector) remaining).iterator() : Iterators.singleton((ZNodeLabel) remaining);
                node = OverlayNode.putIfAbsent(parent, labels);
            }
        } else {
            node = OverlayNode.putIfAbsent(trie, path);
        }
        
        // just added this node
        if (node.getValue() == null) {
            if (parent == null) {
                for (parent = node.parent().get(); (parent != null) && (parent.getValue() == null); parent = parent.parent().get()) {
                    ;
                }
                if (parent != null) {
                    remaining = path.suffix(parent.path().length());
                }
            }
            // fix overlay
            if (parent != null) {
                Iterator<Map.Entry<ZNodeName, OverlayNode<Identifier>>> overlays = parent.getOverlay().entrySet().iterator();
                while (overlays.hasNext()) {
                    Map.Entry<ZNodeName, OverlayNode<Identifier>> e = overlays.next();
                    if (e.getKey().startsWith(remaining)) {
                        overlays.remove();
                        ZNodeName suffix = ((ZNodeLabelVector) e.getKey()).suffix(remaining.length());
                        assert (! (suffix instanceof EmptyZNodeLabel));
                        node.getOverlay().put(suffix, e.getValue());
                    }
                }
                parent.getOverlay().put(remaining, node);
            }
        }
        
        node.setValue(StampedValue.valueOf(stamp, id));
        
        return true;
    }

    public boolean remove(final ZNodePath path, final Identifier id) {
        OverlayNode<Identifier> node = trie.get(path);
        if (node != null) {
            if (node.getValue() == null) {
                return false;
            }
            
            // TODO ??
            assert (id.equals(node.getValue().get()));
            
            OverlayNode<Identifier> parent;
            for (parent = node.parent().get(); (parent != null) && (parent.getValue() == null); parent = parent.parent().get()) {
                ;
            }
            if (parent != null) {
                // fix overlay
                ZNodeName remaining = path.suffix(parent.path());
                assert (parent.getOverlay().containsKey(remaining));
                parent.getOverlay().remove(remaining);
                for (Entry<ZNodeName, OverlayNode<Identifier>> e: node.getOverlay().entrySet()) {
                    parent.getOverlay().put(RelativeZNodePath.fromString(
                            ZNodePath.join(remaining.toString(), e.getKey().toString())),
                            e.getValue());
                }
            }

            node.getOverlay().clear();
            node.setValue(null);
            
            // prune empty leaves
            OverlayNode<Identifier> finger = node;
            while ((finger != null) && finger.isEmpty() && (finger.getValue() == null)) {
                parent = finger.parent().get();
                if (parent != null) {
                    finger.remove();
                    finger = parent;
                } else {
                    break;
                }
            }
        }
        return (node != null);
    }

    public void clear() {
        trie.clear();
    }
    
    @Override
    public String toString() {
        return trie.toString();
    }
    
    private OverlayNode<Identifier> get(final ZNodePath path) {
        OverlayNode<Identifier> enclosing = trie.root();
        OverlayNode<Identifier> node = enclosing;
        ZNodeName remaining = path.suffix(node.path());
        while ((node != null) && !(remaining instanceof EmptyZNodeLabel)) {
            OverlayNode<Identifier> next = null;
            if (node.getValue() != null) {
                enclosing = node;
                next = node.getOverlay().get(remaining);
                if (next != null) {
                    // done
                    enclosing = next;
                    remaining = EmptyZNodeLabel.getInstance();
                } else if (remaining instanceof ZNodeLabelVector) {
                    // path may be a descendant of one of this volume's leaves
                    RelativeZNodePath relative = (RelativeZNodePath) remaining;
                    for (Map.Entry<ZNodeName, OverlayNode<Identifier>> child: node.getOverlay().entrySet()) {
                        if (relative.startsWith(child.getKey())) {
                            next = child.getValue();
                            enclosing = next;
                            remaining = relative.suffix(child.getKey().length());
                            break;
                        }
                    }
                }
            }
            if (next == null) {
                // overlay failed; fall back to trie links
                ZNodeLabel label;
                if (remaining instanceof ZNodeLabelVector) {
                    RelativeZNodePath relative = (RelativeZNodePath) remaining;
                    label = relative.iterator().next();
                    remaining = relative.suffix(label.length());
                } else {
                    label = (ZNodeLabel) remaining;
                    remaining = EmptyZNodeLabel.getInstance();
                }
                next = node.get(label);
            }
            node = next;
        }
        return ((enclosing == null) || (enclosing.getValue() == null)) ? null : enclosing;     
    }
}
