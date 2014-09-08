package edu.uw.zookeeper.safari.control.schema;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;

import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;

public class AvailableIdentifier<O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>,U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> extends EntityLookup<O,V,T,U,C> implements Callable<Identifier> {

    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>,U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> ListenableFuture<Identifier> sync(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>,O> materializer) {
        AvailableIdentifier<O,V,T,U,C> instance = create(value, type, materializer);
        ZNodePath path = instance.directory();
        return Futures.transform(
                SubmittedRequests.submitRequests(
                        materializer,
                        Operations.Requests.sync().setPath(path).build(),
                        Operations.Requests.getChildren().setPath(path).build()), 
                instance.new Callback());
    }
    
    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>,U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> AvailableIdentifier<O,V,T,U,C> create(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>,O> materializer) {
        return new AvailableIdentifier<O,V,T,U,C>(value, type, materializer);
    }
    
    protected AvailableIdentifier(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>,O> materializer) {
        super(value, type, materializer);
    }
    
    @Override
    public Identifier call() throws Exception {
        final LockableZNodeCache<ControlZNode<?>,?,?> cache = materializer().cache();
        cache.lock().readLock().lock();
        try {
            @SuppressWarnings("unchecked")
            C directory = (C) cache.cache().get(directory());
            Hash.Hashed hashed = directory.hasher().apply(value());
            Identifier id = hashed.asIdentifier();
            while (id.equals(Identifier.zero()) || directory.containsKey(ZNodeLabel.fromString(id.toString()))) {
                hashed = hashed.rehash();
                id = hashed.asIdentifier();
            }
            return id;
        } finally {
            cache.lock().readLock().unlock();
        }
    }

    protected class Callback implements AsyncFunction<List<O>,Identifier> {
        
        public Callback() {}
        
        @Override
        public ListenableFuture<Identifier> apply(List<O> input)
                throws Exception {
            for (O response: input) {
                Operations.unlessError(response.record());
            }
            return Futures.immediateFuture(call());
        }
    }
}
