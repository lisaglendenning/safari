package edu.uw.zookeeper.safari.control.schema;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;

public class CreateEntity<O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> extends EntityLookup<O,V,T,U,C> implements Callable<ListenableFuture<Identifier>> {

    @SuppressWarnings("unchecked")
    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>>
    ListenableFuture<Identifier> sync(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        CreateEntity<O,V,T,U,C> instance = create(value, type, materializer);
        ZNodePath path = instance.directory();
        return Futures.transform(
                SubmittedRequests.submit(
                        materializer,
                        PathToRequests.forRequests(
                                Operations.Requests.sync(),
                                Operations.Requests.getChildren()).apply(path)), 
                instance.new Callback(), 
                SameThreadExecutor.getInstance());
    }

    public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>>
    CreateEntity<O,V,T,U,C> create(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        return new CreateEntity<O,V,T,U,C>(value, type, materializer);
    }
    
    protected CreateEntity(
            V value,
            Class<C> type,
            Materializer<ControlZNode<?>, O> materializer) {
        super(value, type, materializer);
    }
    
    @SuppressWarnings("unchecked")
    public ListenableFuture<Identifier> call() throws Exception {
        // TODO DRY with LookupEntity
        final LockableZNodeCache<ControlZNode<?>,?,?> cache = materializer().cache();
        List<Records.Request> requests = ImmutableList.of();
        cache.lock().readLock().lock();
        try {
            C directory = (C) cache.cache().get(directory());
            final ZNodeName valueName = materializer().schema().apply(directory.hashedType()).parent().name();
            Hash.Hashed hashed = directory.hasher().apply(value());
            ZNodeLabel label;
            do {
                Identifier id = hashed.asIdentifier();
                while (id.equals(Identifier.zero())) {
                    hashed = hashed.rehash();
                    id = hashed.asIdentifier();
                }
                label = ZNodeLabel.fromString(id.toString());
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
                            return Futures.immediateFuture(entity.name());
                        } else {
                            hashed = hashed.rehash();
                        }
                    }
                } else {
                    break;
                }
            } while (true);
            
            if (requests.isEmpty()) {
                ZNodePath path = directory.path().join(label);
                requests = ImmutableList.<Records.Request>of(
                        Operations.Requests.multi()
                            .add(materializer().create(path).get())
                            .add(materializer().create(
                                    path.join(valueName), value()).get())
                            .build());
            }
        } finally {
            cache.lock().readLock().unlock();
        }

        return Futures.transform(
                SubmittedRequests.submit(
                    materializer(), 
                    requests),
                new Callback(),
                SameThreadExecutor.getInstance());
    }
    
    protected class Callback implements AsyncFunction<List<O>, Identifier> {

        public Callback() { }
        
        @Override
        public ListenableFuture<Identifier> apply(List<O> input) throws Exception {
            for (Operation.ProtocolResponse<?> response: input) {
                Records.Response record = Operations.unlessError(response.record());
                if (record instanceof IMultiResponse) {
                    Operation.Error error = null;
                    for (Records.MultiOpResponse e: (IMultiResponse) record) {
                        if (e instanceof Operation.Error) {
                            error = (Operation.Error) e;
                            switch (error.error()) {
                            case OK:
                            case NODEEXISTS:
                            case RUNTIMEINCONSISTENCY:
                                break;
                            default:
                                throw KeeperException.create(error.error());
                            }
                        }
                    }
                }
            }
            return call();
        }
    }
}
