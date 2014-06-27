package edu.uw.zookeeper.safari.control.schema;

import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;

public abstract class EntityLookup<O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>,U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> {

    protected final V value;
    protected final Class<C> type;
    protected final Materializer<ControlZNode<?>,O> materializer;
    
    protected EntityLookup(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>,O> materializer) {
        this.value = value;
        this.type = type;
        this.materializer = materializer;
    }
    
    public V value() {
        return value;
    }

    public ZNodePath directory() {
        return materializer.schema().apply(type).path();
    }

    public Materializer<ControlZNode<?>, O> materializer() {
        return materializer;
    }
}
