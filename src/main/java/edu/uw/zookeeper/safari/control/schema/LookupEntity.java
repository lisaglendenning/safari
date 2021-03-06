package edu.uw.zookeeper.safari.control.schema;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;

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

    @SuppressWarnings("unchecked")
    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>>
    ListenableFuture<Optional<Identifier>> sync(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        LookupEntity<O,V,T,U,C> instance = create(value, type, materializer);
        ZNodePath path = instance.directory();
        return Futures.transform(
                SubmittedRequests.submit(
                        materializer,
                        PathToRequests.forRequests(
                                Operations.Requests.sync(),
                                Operations.Requests.getChildren()).apply(path)), 
                instance.new Callback());
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
        List<Records.Request> requests = ImmutableList.of();
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
                        requests = PathToRequests.forRequests(
                                Operations.Requests.sync(),
                                Operations.Requests.getData()).apply(path);
                        break;
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

            if (requests.isEmpty()) {
                // fall back to a linear scan
                requests = Lists.newLinkedList();
                PathToRequests toRequests = PathToRequests.forRequests(
                        Operations.Requests.sync(),
                        Operations.Requests.getData());
                for (ControlZNode<?> entity: directory.values()) {
                    T value = (T) entity.get(valueName);
                    if ((value == null) || (value.data().stamp() < 0L)) {
                        ZNodePath path = entity.path().join(valueName);
                        requests.addAll(toRequests.apply(path)); 
                    } else {
                        if (value().equals(value.data().get())) {
                            return Futures.immediateFuture(Optional.of(((U) entity).name()));
                        }
                    }
                }
            }
        } finally {
            cache.lock().readLock().unlock();
        }
        
        if (requests.isEmpty()) {
            return Futures.immediateFuture(Optional.<Identifier>absent());
        } else {
            return Futures.transform(
                    SubmittedRequests.submit(
                            materializer(), requests),
                    new Callback());
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
