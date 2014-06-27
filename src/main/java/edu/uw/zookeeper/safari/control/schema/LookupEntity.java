package edu.uw.zookeeper.safari.control.schema;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;

public class LookupEntity<O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>,U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> extends EntityLookup<O,V,T,U,C> implements Callable<ListenableFuture<Optional<Identifier>>> {

    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>>
    ListenableFuture<Optional<Identifier>> sync(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        LookupEntity<O,V,T,U,C> instance = create(value, type, materializer);
        ZNodePath path = instance.directory();
        return Futures.transform(
                SubmittedRequests.submitRequests(
                        materializer,
                        Operations.Requests.sync().setPath(path).build(),
                        Operations.Requests.getChildren().setPath(path).build()), 
                instance.new Callback(), 
                SameThreadExecutor.getInstance());
    }

    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>>
    LookupEntity<O,V,T,U,C> create(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        return new LookupEntity<O,V,T,U,C>(value, type, materializer);
    }

    protected LookupEntity(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        super(value, type, materializer);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<Optional<Identifier>> call() throws Exception {
        final LockableZNodeCache<ControlZNode<?>,?,?> cache = materializer().cache();
        cache.lock().readLock().lock();
        try {
            C directory = (C) cache.cache().get(directory());
            final ZNodeName valueName = materializer().schema().apply(directory.hashedType()).parent().name();
            Hash.Hashed hashed = directory.hasher().apply(value());
            do {
                Identifier id = hashed.asIdentifier();
                while (id.equals(Identifier.zero())) {
                    hashed = hashed.rehash();
                    id = hashed.asIdentifier();
                }
                ZNodeLabel label = ZNodeLabel.fromString(id.toString());
                U entity = (U) directory.get(label);
                if (entity != null) {
                    T value = (T) entity.get(valueName);
                    if ((value == null) || (value.data().stamp() < 0L)) {
                        ZNodePath path = entity.path().join(valueName);
                        return Futures.transform(
                                SubmittedRequests.submitRequests(
                                    materializer(), 
                                    Operations.Requests.sync().setPath(path).build(), 
                                    Operations.Requests.getData().setPath(path).build()),
                                new Callback(),
                                SameThreadExecutor.getInstance());
                    } else {
                        if (value().equals(value.data().get())) {
                            return Futures.immediateFuture(Optional.of(entity.name()));
                        } else {
                            hashed = hashed.rehash();
                        }
                    }
                } else {
                    break;
                }
            } while (true);
            
            // fall back to a linear scan
            List<Records.Request> requests = Lists.newLinkedList();
            for (ControlZNode<?> entity: directory.values()) {
                T value = (T) entity.get(valueName);
                if ((value == null) || (value.data().stamp() < 0L)) {
                    ZNodePath path = entity.path().join(valueName);
                    requests.add(Operations.Requests.sync().setPath(path).build()); 
                    requests.add(Operations.Requests.getData().setPath(path).build());
                } else {
                    if (value().equals(value.data().get())) {
                        return Futures.immediateFuture(Optional.of(((U) entity).name()));
                    }
                }
            }
            
            if (requests.isEmpty()) {
                return Futures.immediateFuture(Optional.<Identifier>absent());
            } else {
                return Futures.transform(
                        SubmittedRequests.submit(
                                materializer(), requests),
                        new Callback(),
                        SameThreadExecutor.getInstance());
            }
        } finally {
            cache.lock().readLock().unlock();
        }
    }
    
    protected class Callback implements AsyncFunction<List<O>, Optional<Identifier>> {

        public Callback() { }
        
        @Override
        public ListenableFuture<Optional<Identifier>> apply(List<O> input) throws Exception {
            for (Operation.ProtocolResponse<?> response: input) {
                Operations.unlessError(response.record());
            }
            return call();
        }
    }
}
