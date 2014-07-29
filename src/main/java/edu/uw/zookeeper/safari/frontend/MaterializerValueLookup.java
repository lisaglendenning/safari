package edu.uw.zookeeper.safari.frontend;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SharedLookup;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public class MaterializerValueLookup<T,V> extends SimpleToStringListenableFuture<List<Operation.ProtocolResponse<?>>> implements Callable<Optional<V>>, Runnable {

    public static <T,V> CachedFunction<T, V> newCachedFunction(
            final Function<T, ZNodePath> paths,
            final Materializer<ControlZNode<?>,?> materializer) {
        final Function<T, V> cached = new Function<T, V>() {
            @SuppressWarnings("unchecked")
            @Override
            public V apply(T input) {
                materializer.cache().lock().readLock().lock();
                try {
                    ControlZNode<?> node = materializer.cache().cache().get(
                            paths.apply(input));
                    if (node != null) {
                        return (V) node.data().get();
                    }
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
                return null;
            }
        };
        @SuppressWarnings("unchecked")
        final PathToQuery<?,?> query = PathToQuery.forRequests(
                materializer, 
                Operations.Requests.sync(), 
                Operations.Requests.getData());
        return CachedFunction.create(
                cached, 
                SharedLookup.create(
                        new AsyncFunction<T, V>() {
                            @Override
                            public ListenableFuture<V> apply(
                                    final T input)
                                    throws Exception {
                                final ZNodePath path = paths.apply(input);
                                final Promise<V> promise = SettableFuturePromise.create();
                                lookup(input, Futures.<Operation.ProtocolResponse<?>>allAsList(query.apply(path).call()), cached, promise);
                                return promise;
                            }
                        }), 
                LogManager.getLogger(MaterializerValueLookup.class));
    }

    protected static <T,V> MaterializerValueLookup<T,V> lookup(
            T input, 
            ListenableFuture<List<Operation.ProtocolResponse<?>>> future, 
            Function<T, V> cached,
            Promise<V> promise) {
        return new MaterializerValueLookup<T,V>(input, future, cached, promise);
    }
    
    private final Function<T, V> cached;
    private final T input;
    private final CallablePromiseTask<?, V> delegate;
    
    protected MaterializerValueLookup(
            T input, 
            ListenableFuture<List<Operation.ProtocolResponse<?>>> future, 
            Function<T, V> cached,
            Promise<V> promise) {
        super(future);
        this.cached = cached;
        this.input = input;
        this.delegate = CallablePromiseTask.create(this, promise);
        addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public void run() {
        delegate.run();
    }

    @Override
    public Optional<V> call() throws Exception {
        if (isDone()) {
            List<Operation.ProtocolResponse<?>> responses = get();
            for (Operation.ProtocolResponse<?> response: responses) {
                Operations.unlessError(response.record());
            }
            return Optional.of(cached.apply(input));
        }
        return Optional.absent();
    }
    
    @Override
    protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(input)).addValue(toString(delegate));
    }
}
