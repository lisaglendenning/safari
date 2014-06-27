package edu.uw.zookeeper.safari.schema;

import java.util.Map;

import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class SafariZNode<E extends SafariZNode<E,?>, V> extends Materializer.MaterializedNode<E,V> {

    protected SafariZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            NameTrie.Pointer<? extends E> parent) {
        super(schema, codec, parent);
    }

    protected SafariZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends E> parent) {
        super(schema, codec, data, stat, stamp, parent);
    }

    protected SafariZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends E> parent,
            Map<ZNodeName, E> children) {
        super(schema, codec, data, stat, stamp, parent, children);
    }
}
