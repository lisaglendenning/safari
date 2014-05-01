package edu.uw.zookeeper.safari.data;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.common.CachedFunction;

public abstract class GetDataFromMaterializer {
    
    public static <V> CachedFunction<ZNodePath, V> create(
            final Materializer<?,?> materializer) {
        @SuppressWarnings("unchecked")
        final Function<ZNodePath, V> cached = new Function<ZNodePath, V>() {
            @Override
            public @Nullable V apply(final ZNodePath path) {
                materializer.cache().lock().readLock().lock();
                try {
                    Materializer.MaterializedNode<?,?> node = materializer.cache().cache().get(path);
                    if (node != null) {
                        return (V) node.data().get();
                    }
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
                return null;
            }
        };
        final AsyncFunction<ZNodePath, V> async = new AsyncFunction<ZNodePath, V>() {
            @Override
            public ListenableFuture<V> apply(final ZNodePath path) {
                materializer.sync(path).call();
                return Futures.transform(materializer.getData(path).call(), 
                        new AsyncFunction<Operation.ProtocolResponse<?>, V>() {
                            @Override
                            public ListenableFuture<V> apply(
                                    Operation.ProtocolResponse<?> response) throws Exception {
                                Operations.unlessError(response.record());
                                return Futures.immediateFuture(cached.apply(path));
                            }
                        });
            }
        };
        return CachedFunction.create(cached, async, LogManager.getLogger(GetDataFromMaterializer.class));
    }
    
    private GetDataFromMaterializer() {}
}
