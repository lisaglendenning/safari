package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class LookupHashedTask<V> extends RunnablePromiseTask<AsyncFunction<Identifier, Optional<V>>, V> {

    public static <V> LookupHashedTask<V> newInstance(
            Hash.Hashed hashed,
            AsyncFunction<Identifier, Optional<V>> lookup) {
        Promise<V> promise = SettableFuturePromise.create();
        return new LookupHashedTask<V>(hashed, lookup, promise);
    }
    
    protected ListenableFuture<Optional<V>> pending;
    protected Hash.Hashed hashed;
    
    public LookupHashedTask(
            Hash.Hashed hashed,
            AsyncFunction<Identifier, Optional<V>> lookup,
            Promise<V> promise) {
        super(lookup, promise);
        this.hashed = checkNotNull(hashed);
        this.pending = null;
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancel = super.cancel(mayInterruptIfRunning);
        if (cancel) {
            if (pending != null) {
                pending.cancel(mayInterruptIfRunning);
            }
        }
        return cancel;
    }

    @Override
    public synchronized Optional<V> call() throws Exception {
        if (pending == null) {
            Identifier id = hashed.asIdentifier();
            while (id.equals(Identifier.zero())) {
                hashed = hashed.rehash();
                id = hashed.asIdentifier();
            }
            pending = task().apply(id);
            pending.addListener(this, SameThreadExecutor.getInstance());
            return Optional.absent();
        }
        
        if (!pending.isDone()) {
            return Optional.absent();
        } else if (pending.isCancelled()) {
            cancel(true);
            return Optional.absent();
        }
        
        Optional<V> result = pending.get();
        if (!result.isPresent()) {
            // try again
            hashed = hashed.rehash();
            pending = null;
            run();
        }
        return result;
    }
}