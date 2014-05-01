package edu.uw.zookeeper.safari.data;

import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;

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
public final class VolumeOverlay {
    
    public static VolumeOverlay newInstance() {
        return new VolumeOverlay(SimpleLabelTrie.forRoot(VolumeOverlayNode.root(null)));
    }

    private final NameTrie<VolumeOverlayNode> trie;
    
    protected VolumeOverlay(
            NameTrie<VolumeOverlayNode> trie) {
        this.trie = trie;
    }
    
    public @Nullable VolumeOverlayNode apply(final ZNodePath path) {
        VolumeOverlayNode enclosing = trie.root();
        VolumeOverlayNode node = enclosing;
        ZNodeName remaining = path.suffix(0);
        while ((node != null) && !(remaining instanceof EmptyZNodeLabel)) {
            VolumeOverlayNode next = null;
            if (node.getId() != null) {
                enclosing = node;
                next = node.getOverlay().get(remaining);
                if (next != null) {
                    // done; path is a leaf of this volume
                    remaining = EmptyZNodeLabel.getInstance();
                } else if (remaining instanceof ZNodeLabelVector) {
                    assert (remaining instanceof RelativeZNodePath);
                    // path may be a descendant of one of this volume's leaves
                    RelativeZNodePath relative = (RelativeZNodePath) remaining;
                    for (Map.Entry<ZNodeName, VolumeOverlayNode> child: node.getOverlay().entrySet()) {
                        if (relative.startsWith(child.getKey())) {
                            next = child.getValue();
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
                    assert (remaining instanceof RelativeZNodePath);
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
        if ((enclosing == null) || (enclosing.getId() == null)) {
            return null;
        }
        return enclosing;     
    }

    public VolumeOverlayNode add(final long stamp, final ZNodePath path, final Identifier id) {
        // don't overwrite more up-to-date information
        VolumeOverlayNode node = apply(path);
        VolumeOverlayNode parent = null;
        ZNodeName remaining = null;
        if (node != null) {
            if (node.path().length() == path.length()) {
                // my path
                if (node.getId().stamp() >= stamp) {
                    assert ((node.getId().stamp() > stamp) || id.equals(node.getId().get()));
                    return null;
                }
            } else {
                // parent
                parent = node;
                // TODO ??
//                if (parent.getId().stamp() > stamp) {
//                    return null;
//                }
                remaining = path.suffix(parent.path().length());
                Iterator<ZNodeLabel> labels = (remaining instanceof ZNodeLabelVector) ? ((ZNodeLabelVector) remaining).iterator() : Iterators.singleton((ZNodeLabel) remaining);
                node = VolumeOverlayNode.putIfAbsent(parent, labels);
            }
        } else {
            node = VolumeOverlayNode.putIfAbsent(trie, path);
        }
        
        if (node.getId() == null) {
            if (parent == null) {
                for (parent = node.parent().get(); (parent != null) && (parent.getId() == null); parent = parent.parent().get()) {
                    ;
                }
                if (parent != null) {
                    remaining = path.suffix(parent.path().length());
                }
            }
            // fix parent's overlay
            if (parent != null) {
                assert (! parent.getOverlay().containsKey(remaining));
                Iterator<Map.Entry<ZNodeName, VolumeOverlayNode>> overlays = parent.getOverlay().entrySet().iterator();
                while (overlays.hasNext()) {
                    Map.Entry<ZNodeName, VolumeOverlayNode> e = overlays.next();
                    if (e.getKey().startsWith(remaining)) {
                        overlays.remove();
                        ZNodeName suffix = ((ZNodeLabelVector) e.getKey()).suffix(remaining.length());
                        node.getOverlay().put(suffix, e.getValue());
                    }
                }
                parent.getOverlay().put(remaining, node);
            }
        }
        
        node.setId(StampedValue.valueOf(stamp, id));
        
        return node;
    }

    public VolumeOverlayNode remove(final ZNodePath path, final Identifier id) {
        VolumeOverlayNode node = trie.get(path);
        if (node != null) {
            if (node.getId() == null) {
                return null;
            }
            
            // TODO ??
            assert (id.equals(node.getId().get()));
            
            // fix parent's overlay
            VolumeOverlayNode parent;
            for (parent = node.parent().get(); (parent != null) && (parent.getId() == null); parent = parent.parent().get()) {
                ;
            }
            if (parent != null) {
                ZNodeName remaining = path.suffix(parent.path().length());
                assert (parent.getOverlay().containsKey(remaining));
                parent.getOverlay().remove(remaining);
                parent.getOverlay().putAll(node.getOverlay());
            }

            node.getOverlay().clear();
            node.setId(null);
            
            // prune empty leaves
            VolumeOverlayNode finger = node;
            while ((finger != null) && finger.isEmpty() && (finger.getId() == null)) {
                parent = finger.parent().get();
                if (parent != null) {
                    finger.remove();
                    finger = parent;
                }
            }
        }
        return node;
    }

    public void clear() {
        trie.clear();
    }
}
