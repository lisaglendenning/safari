package edu.uw.zookeeper.safari.storage.snapshot;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Comparator;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.DefaultsNode;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.ZNodeName;

/**
 * For use in a label trie.
 */
public final class SequentialNode<V> extends DefaultsNode.AbstractDefaultsNode<SequentialNode<V>> {

    public static <V> SequentialNode<V> putIfAbsent(NameTrie<SequentialNode<V>> trie, AbsoluteZNodePath path, V value) {
        return putIfAbsent(trie, path.parent()).putIfAbsent(path.label(), value);
    }

    public static <V> SequentialNode<V> putIfAbsent(NameTrie<SequentialNode<V>> trie, AbsoluteZNodePath path, V value, Sequential<String,?> sequential) {
        return putIfAbsent(trie, path.parent()).putIfAbsent(path.label(), value, sequential);
    }
    
    public static <V> SequentialNode<V> sequentialRoot() {
        return new SequentialNode<V>(
                AbstractNameTrie.<SequentialNode<V>>rootPointer());
    }
    
    public static <V> SequentialNode<V> sequentialChild(ZNodeName label, SequentialNode<V> parent) {
        return new SequentialNode<V>(
                AbstractNameTrie.<SequentialNode<V>>weakPointer(label, parent));
    }
    
    public static <V> SequentialNode<V> sequentialChild(ZNodeName label, SequentialNode<V> parent, V value) {
        return new SequentialNode<V>(
                value,
                AbstractNameTrie.<SequentialNode<V>>weakPointer(label, parent));
    }
    
    public static <V> SequentialNode<V> sequentialChild(ZNodeName label, SequentialNode<V> parent, V value, Sequential<String,?> sequential) {
        checkArgument(Objects.equal(Sequential.fromString(label.toString()), sequential));
        return new SequentialNode<V>(
                sequential,
                value,
                AbstractNameTrie.<SequentialNode<V>>weakPointer(label, parent));
    }
    
    public static Comparator<ZNodeName> comparator() {
        return COMPARATOR;
    }

    protected static final Comparator<ZNodeName> COMPARATOR = new Comparator<ZNodeName>() {
        private final Comparator<String> delegate = Sequential.comparator();
        @Override
        public int compare(final ZNodeName a, final ZNodeName b) {
            return delegate.compare(a.toString(), b.toString());
        }
    };

    private final Optional<? extends Sequential<String,?>> sequential;
    private final Optional<V> value;

    private SequentialNode(
            NameTrie.Pointer<? extends SequentialNode<V>> parent) {
        this(Optional.<Sequential<String,?>>absent(), Optional.<V>absent(), parent);
    }

    private SequentialNode(
            V value,
            NameTrie.Pointer<? extends SequentialNode<V>> parent) {
        this(Sequential.fromString(parent.name().toString()), value, parent);
    }

    private SequentialNode(
            Sequential<String,?> sequential,
            V value,
            NameTrie.Pointer<? extends SequentialNode<V>> parent) {
        this(Optional.of(sequential), Optional.of(value), parent);
    }
    
    private SequentialNode(
            Optional<? extends Sequential<String,?>> sequential,
            Optional<V> value,
            NameTrie.Pointer<? extends SequentialNode<V>> parent) {
        super(parent, Maps.<ZNodeName, ZNodeName, SequentialNode<V>>newTreeMap(COMPARATOR));
        this.sequential = sequential;
        this.value = value;
    }
    
    public Sequential<String,?> getSequential() {
        return sequential.get();
    }
    
    public V getValue() {
        return value.get();
    }
    
    public SequentialNode<V> putIfAbsent(ZNodeName label, V value) {
        SequentialNode<V> child = get(label);
        if (child == null) {
            child = sequentialChild(label, this, value);
            put(label, child);
        } else {
            checkArgument(Objects.equal(child.getValue(), value));
        }
        return child;
    }
    
    public SequentialNode<V> putIfAbsent(ZNodeName label, V value, Sequential<String,?> sequential) {
        SequentialNode<V> child = get(label);
        if (child == null) {
            child = sequentialChild(label, this, value, sequential);
            put(label, child);
        } else {
            checkArgument(Objects.equal(child.getSequential(), sequential));
            checkArgument(Objects.equal(child.getValue(), value));
        }
        return child;
    }

    @Override
    protected SequentialNode<V> newChild(ZNodeName label) {
        return sequentialChild(label, this);
    }
    
    @Override
    protected MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper toString) {
        toString = super.toString(toString);
        if (sequential.isPresent()) {
            toString = toString.add("value", getValue()).add("sequential", getSequential());
        }
        return toString;
    }
}
