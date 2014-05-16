package edu.uw.zookeeper.safari.backend;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.storage.StorageSchema;
import edu.uw.zookeeper.safari.storage.StorageZNode;

public class SessionLookup extends PromiseTask<Long, StorageSchema.Safari.Sessions.Session.Data> implements Callable<Optional<StorageSchema.Safari.Sessions.Session.Data>> {
    
    public static SessionLookup create(
            Long session, 
            Materializer<StorageZNode<?>,?> materializer,
            Promise<StorageSchema.Safari.Sessions.Session.Data> promise) {
        return new SessionLookup(session, materializer, promise);
    }
    
    private final Materializer<StorageZNode<?>,?> materializer;
    private final ListenableFuture<? extends Operation.ProtocolResponse<?>> lookup;
    
    public SessionLookup(
            Long session, 
            Materializer<StorageZNode<?>,?> materializer,
            Promise<StorageSchema.Safari.Sessions.Session.Data> promise) {
        super(session, promise);
        // TODO check cache first
        this.materializer = materializer;
        this.lookup = materializer.submit(Operations.Requests.getData().setPath(path()).build());
        this.lookup.addListener(CallablePromiseTask.create(this, this), SameThreadExecutor.getInstance());
    }
    
    public ZNodePath path() {
        return StorageSchema.Safari.Sessions.Session.pathOf(task());
    }

    @Override
    public Optional<StorageSchema.Safari.Sessions.Session.Data> call() throws Exception {
        if (! lookup.isDone()) {
            return Optional.absent();
        }
        try {
            Operations.unlessError(lookup.get().record());
            materializer.cache().lock().readLock().lock();
            try {
                return Optional.of((StorageSchema.Safari.Sessions.Session.Data) materializer.cache().cache().get(path()).data().get());
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }
}