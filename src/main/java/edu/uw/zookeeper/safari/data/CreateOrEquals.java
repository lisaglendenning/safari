package edu.uw.zookeeper.safari.data;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class CreateOrEquals<V> extends ForwardingPromise<Optional<V>> implements Runnable {
    
    public static <V> CreateOrEquals<V> create(ZNodePath path, V value, Materializer<?,?> materializer) {
        return new CreateOrEquals<V>(path, value, materializer, SettableFuturePromise.<Optional<V>>create());
    }
    
    protected final Materializer<?,?> materializer;
    protected final ZNodePath path;
    protected final V value;
    protected final Promise<Optional<V>> delegate;
    protected final ListenableFuture<? extends Operation.ProtocolResponse<?>> future;
    protected GetData getData = null;
    
    protected CreateOrEquals(
            ZNodePath path,
            V value,
            Materializer<?,?> materializer,
            Promise<Optional<V>> delegate) {
        this.materializer = materializer;
        this.path = path;
        this.value = value;
        this.delegate = delegate;
        this.getData = null;
        this.future = materializer.create(path, value).call();
        future.addListener(this, SameThreadExecutor.getInstance());
        delegate.addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public synchronized void run() {
        if (isDone()) {
            if (isCancelled()) {
                if (getData != null) {
                    getData.future.cancel(false);
                }
            }
            return;
        }
        if (! future.isDone()) {
            return;
        }
        try {
            Optional<Operation.Error> error = Operations.maybeError(future.get().record(), KeeperException.Code.NODEEXISTS);
            if (error.isPresent()) {
                if (getData == null) {
                    getData = new GetData();
                }
            } else {
                // successful create
                set(Optional.<V>absent());
            }
        } catch (Exception e) {
            setException(e);
        }
    }
    
    @Override
    protected Promise<Optional<V>> delegate() {
        return delegate;
    }

    protected class GetData implements Runnable {

        protected final ListenableFuture<? extends Operation.ProtocolResponse<?>> future;
        
        public GetData() {
            this.future = materializer.getData(path).call();
            future.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void run() {
            if (! future.isDone()) {
                return;
            }
            try {
                Operations.unlessError(future.get().record());
            } catch (Exception e) {
                setException(e);
                return;
            }
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
            set(result);
        }
    }
}