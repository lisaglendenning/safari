package edu.uw.zookeeper.client;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class CreateOrEquals<V> implements Function<List<ListenableFuture<? extends Optional<V>>>, Optional<? extends ListenableFuture<? extends Optional<V>>>> {
    
    public static <V> ListenableFuture<Optional<V>> create(
            ZNodePath path, 
            V value, 
            Materializer<?,?> materializer, 
            Promise<Optional<V>> promise) {
        return ChainedFutures.run(
                ChainedFutures.process(
                    ChainedFutures.chain(
                            new CreateOrEquals<V>(path, value, materializer),
                            Lists.<ListenableFuture<? extends Optional<V>>>newArrayListWithCapacity(2)),
                    ChainedFutures.<Optional<V>,ListenableFuture<? extends Optional<V>>>getLast()), 
                promise);
    }
    
    protected final Materializer<?,?> materializer;
    protected final ZNodePath path;
    protected final V value;
    
    protected CreateOrEquals(
            ZNodePath path,
            V value,
            Materializer<?,?> materializer) {
        this.materializer = materializer;
        this.path = path;
        this.value = value;
    }
    
    @Override
    public Optional<? extends ListenableFuture<? extends Optional<V>>> apply(
            List<ListenableFuture<? extends Optional<V>>> input) {
        switch (input.size()) {
        case 0:
        {
            return Optional.of(
                    Futures.transform(
                            materializer.create(path, value).call(),
                            new AsyncFunction<Operation.ProtocolResponse<?>,Optional<V>>() {
                                @Override
                                public ListenableFuture<Optional<V>> apply(
                                        Operation.ProtocolResponse<?> input)
                                        throws Exception {
                                    Operations.unlessError(input.record());
                                    // successful create
                                    return Futures.immediateFuture(Optional.<V>absent());
                                }
                            },
                            SameThreadExecutor.getInstance()));
        }
        case 1:
        {
            try {
                input.get(0).get(0L, TimeUnit.MILLISECONDS);
                return Optional.absent();
            } catch (ExecutionException e) {
                if (! (e.getCause() instanceof KeeperException.NodeExistsException)) {
                    return Optional.absent();
                }
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } catch (TimeoutException e) {
                throw new AssertionError(e);
            }
            return Optional.of(
                    Futures.transform(
                            materializer.getData(path).call(),
                            new AsyncFunction<Operation.ProtocolResponse<?>,Optional<V>>() {
                                @Override
                                public ListenableFuture<Optional<V>> apply(
                                        Operation.ProtocolResponse<?> input)
                                        throws Exception {
                                    Operations.unlessError(input.record());
                                    Optional<V> result;
                                    materializer.cache().lock().readLock().lock();
                                    try {
                                        Materializer.MaterializedNode<?,?> node = materializer.cache().cache().get(path);
                                        assert (node != null);
                                        @SuppressWarnings("unchecked")
                                        V existing = (V) node.data().get();
                                        if (Objects.equal(value, existing)) {
                                            // equivalent data
                                            result = Optional.absent();
                                        } else {
                                            result = Optional.of(existing);
                                        }
                                    } finally {
                                        materializer.cache().lock().readLock().unlock();
                                    }
                                    return Futures.immediateFuture(result);
                                }
                            },
                            SameThreadExecutor.getInstance()));
        }
        case 2:
            return Optional.absent();
        default:
            throw new AssertionError();
        }
    }
}