package edu.uw.zookeeper.safari.storage.schema;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public final class SessionLookup<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Callable<Optional<StorageSchema.Safari.Sessions.Session.Data>> {
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<StorageSchema.Safari.Sessions.Session.Data> create(
            Long session, 
            Materializer<StorageZNode<?>,O> materializer) {
        final ZNodePath path = StorageSchema.Safari.Sessions.Session.pathOf(session);
        materializer.cache().lock().readLock().lock();
        try {
            StorageZNode<?> node = materializer.cache().cache().get(path);
            if ((node != null) && (node.data().get() != null)) {
                return Futures.immediateFuture((StorageSchema.Safari.Sessions.Session.Data) node.data().get());
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        final CallablePromiseTask<SessionLookup<O>, StorageSchema.Safari.Sessions.Session.Data> task = CallablePromiseTask.create(
                new SessionLookup<O>(path, materializer),
                SettableFuturePromise.<StorageSchema.Safari.Sessions.Session.Data>create());
        task.task().addListener(task, MoreExecutors.directExecutor());
        return task;
    }
    
    private final ZNodePath path;
    private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
    
    @SuppressWarnings("unchecked")
    protected SessionLookup(
            ZNodePath path, 
            Materializer<StorageZNode<?>,O> materializer) {
        super(SubmittedRequests.<Records.Request,O>submit(
                materializer, 
                PathToRequests.forRequests(
                        Operations.Requests.sync(),
                        Operations.Requests.getData()).apply(path)));
        this.path = path;
        this.cache = materializer.cache();
    }

    @Override
    public Optional<StorageSchema.Safari.Sessions.Session.Data> call() throws Exception {
        if (isDone()) {
            for (Operation.ProtocolResponse<?> response: get()) {
                Operations.unlessError(response.record());
            }
            cache.lock().readLock().lock();
            try {
                return Optional.of((StorageSchema.Safari.Sessions.Session.Data) cache.cache().get(path).data().get());
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        return Optional.absent();
    }
}
