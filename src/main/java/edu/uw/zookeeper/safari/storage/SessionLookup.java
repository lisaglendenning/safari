package edu.uw.zookeeper.safari.storage;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class SessionLookup<O extends Operation.ProtocolResponse<?>> extends ForwardingListenableFuture<List<O>> implements Callable<Optional<StorageSchema.Safari.Sessions.Session.Data>> {
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<StorageSchema.Safari.Sessions.Session.Data> create(
            Long session, 
            Materializer<StorageZNode<?>,O> materializer,
            Promise<StorageSchema.Safari.Sessions.Session.Data> promise) {
        ZNodePath path = StorageSchema.Safari.Sessions.Session.pathOf(session);
        materializer.cache().lock().readLock().lock();
        try {
            StorageZNode<?> node = materializer.cache().cache().get(path);
            if ((node != null) && (node.data().get() != null)) {
                promise.set((StorageSchema.Safari.Sessions.Session.Data) node.data().get());
                return promise;
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        SessionLookup<O> lookup = new SessionLookup<O>(path, materializer, promise);
        CallablePromiseTask<SessionLookup<O>, StorageSchema.Safari.Sessions.Session.Data> task = CallablePromiseTask.create(lookup, promise);
        lookup.addListener(task, SameThreadExecutor.getInstance());
        return task;
    }
    
    private final ZNodePath path;
    private final Materializer<StorageZNode<?>,O> materializer;
    private final SubmittedRequests<Records.Request,O> lookup;
    
    protected SessionLookup(
            ZNodePath path, 
            Materializer<StorageZNode<?>,O> materializer,
            Promise<StorageSchema.Safari.Sessions.Session.Data> promise) {
        this.path = path;
        this.materializer = materializer;
        this.lookup = SubmittedRequests.<Records.Request,O>submitRequests(materializer, 
                Operations.Requests.sync().setPath(path).build(),
                Operations.Requests.getData().setPath(path).build());
    }

    @Override
    public Optional<StorageSchema.Safari.Sessions.Session.Data> call() throws Exception {
        if (! lookup.isDone()) {
            return Optional.absent();
        }
        for (Operation.ProtocolResponse<?> response: lookup.get()) {
            Operations.unlessError(response.record());
        }
        materializer.cache().lock().readLock().lock();
        try {
            return Optional.of((StorageSchema.Safari.Sessions.Session.Data) materializer.cache().cache().get(path).data().get());
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
    }

    @Override
    protected ListenableFuture<List<O>> delegate() {
        return lookup;
    }
}
