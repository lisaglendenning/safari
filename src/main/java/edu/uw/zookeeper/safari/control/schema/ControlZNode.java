package edu.uw.zookeeper.safari.control.schema;

import java.util.Map;

import com.google.common.base.Function;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.schema.SafariZNode;


public abstract class ControlZNode<V> extends SafariZNode<ControlZNode<?>,V> {

    @SuppressWarnings("unchecked")
    public static <V,T extends ControlZNode<V>,U extends IdentifierControlZNode,C extends EntityDirectoryZNode<V,T,U>> Hash.Hashed hash(
            V value,
            ZNodePath directory,
            Materializer<ControlZNode<?>, ?> materializer) {
        materializer.cache().lock().readLock().lock();
        try {
            return ((C) materializer.cache().cache().get(directory)).hasher().apply(value);
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
    }
    
    protected ControlZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            NameTrie.Pointer<? extends ControlZNode<?>> parent) {
        super(schema, codec, parent);
    }

    protected ControlZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends ControlZNode<?>> parent) {
        super(schema, codec, data, stat, stamp, parent);
    }

    protected ControlZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends ControlZNode<?>> parent,
            Map<ZNodeName, ControlZNode<?>> children) {
        super(schema, codec, data, stat, stamp, parent, children);
    }

    public static abstract class NamedControlZNode<V,T> extends ControlZNode<V> {

        protected final T name;

        protected NamedControlZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(schema, codec, parent);
            this.name = name;
        }

        protected NamedControlZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(schema, codec, data, stat, stamp, parent);
            this.name = name;
        }

        protected NamedControlZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent,
                Map<ZNodeName, ControlZNode<?>> children) {
            super(schema, codec, data, stat, stamp, parent, children);
            this.name = name;
        }
        
        public T name() {
            return name;
        }
    }
    
    public static abstract class IdentifierControlZNode extends NamedControlZNode<Void,Identifier> {

        @Name(type=NameType.PATTERN)
        public static final ZNodeLabel LABEL = ZNodeLabel.fromString(Identifier.PATTERN);
        
        protected IdentifierControlZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
        }
        
        protected IdentifierControlZNode(
                Identifier name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(name, schema, codec, null, -1L, parent);
        }

        protected IdentifierControlZNode(
                Identifier name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(name, schema, codec, null, stat, stamp, parent);
        }
    }
    
    public static abstract class EntityDirectoryZNode<V,T extends ControlZNode<V>,U extends IdentifierControlZNode> extends ControlZNode<Void> {
    
        protected EntityDirectoryZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(schema, codec, null, -1L, parent);
        }
    
        protected EntityDirectoryZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(schema, codec, null, stat, stamp, parent);
        }

        public abstract Class<? extends U> entityType();

        public abstract Class<? extends T> hashedType();
        
        public abstract Function<V, Hash.Hashed> hasher();
    }
}
