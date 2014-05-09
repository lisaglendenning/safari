package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class LookupHashedTask<V> extends PromiseTask<AsyncFunction<Identifier, Optional<V>>, V> implements Runnable, Callable<Optional<V>> {

    public static <V> LookupHashedTask<V> create(
            Hash.Hashed hashed,
            AsyncFunction<Identifier, Optional<V>> lookup,
            Promise<V> promise) {
        LookupHashedTask<V> task = new LookupHashedTask<V>(hashed, lookup, promise);
        task.run();
        return task;
    }
    
    protected final CallablePromiseTask<LookupHashedTask<V>, V> delegate;
    protected Optional<ListenableFuture<Optional<V>>> pending;
    protected Hash.Hashed hashed;
    
    public LookupHashedTask(
            Hash.Hashed hashed,
            AsyncFunction<Identifier, Optional<V>> lookup,
            Promise<V> promise) {
        super(lookup, promise);
        this.delegate = CallablePromiseTask.create(this, this);
        this.hashed = checkNotNull(hashed);
        this.pending = Optional.absent();
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public synchronized void run() {
        if (isDone()) {
            if (isCancelled()) {
                if (pending.isPresent()) {
                    pending.get().cancel(false);
                }
            }
        } else {
            delegate.run();
        }
    }

    @Override
    public synchronized Optional<V> call() throws Exception {
        if (!pending.isPresent()) {
            Identifier id = hashed.asIdentifier();
            while (id.equals(Identifier.zero())) {
                hashed = hashed.rehash();
                id = hashed.asIdentifier();
            }
            pending = Optional.of(task().apply(id));
            pending.get().addListener(this, SameThreadExecutor.getInstance());
        } else if (pending.get().isDone()) {
            if (pending.get().isCancelled()) {
                throw new CancellationException();
            }
            
            Optional<V> result = pending.get().get();
            if (!result.isPresent()) {
                // try again
                hashed = hashed.rehash();
                pending = Optional.absent();
                run();
            } else {
                return result;
            }
        }
        
        return Optional.absent();
    }
}