package edu.uw.zookeeper.safari.data;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.client.TreeFetcher.Parameters;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class FetchUntil<V> extends PromiseTask<TreeFetcher.Builder<V>, V> implements FutureCallback<Optional<V>>, SessionListener, Runnable {

    public static <V> FetchUntil<V> newInstance(
            ZNodePath root, 
            Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result, 
            Materializer<?,?> materializer) {
        TreeFetcher.Builder<V> fetcher = TreeFetcher.<V>builder()
                .setParameters(Parameters.of(true, true, false, false))
                .setResult(result).setClient(materializer).setRoot(root);
        Promise<V> promise = newPromise();
        return newInstance(root, materializer, fetcher, promise);
    }

    public static <V> FetchUntil<V> newInstance(
            ZNodePath root, Materializer<?,?> materializer, TreeFetcher.Builder<V> fetcher, Promise<V> promise) {
        return new FetchUntil<V>(
                root, materializer, fetcher, promise);
    }

    public static <V> Promise<V> newPromise() {
        return LoggingPromise.create(LogManager.getLogger(FetchUntil.class), PromiseTask.<V>newPromise());
    }
    
    protected final ZNodePath root;
    protected final Materializer<?,?> materializer;
    
    public FetchUntil(
            ZNodePath root, 
            Materializer<?,?> materializer, 
            TreeFetcher.Builder<V> fetcher,
            Promise<V> promise) {
        super(fetcher, promise);
        this.root = root;
        this.materializer = materializer;
        
        materializer.subscribe(this);
        this.addListener(this, SameThreadExecutor.getInstance());

        materializer.submit(Operations.Requests.sync().setPath(root).build());
        new Updater(root);
    }

    @Override
    public void run() {
        if (isDone()) {
            materializer.unsubscribe(this);
        }
    }

    @Override
    public void onSuccess(Optional<V> result) {
        try {
            if (result.isPresent()) {
                set(result.get());
            } else {
                // force watches
                materializer.submit(Operations.Requests.sync().setPath(root).build());
            }
        } catch (Exception e) {
            setException(e);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }

    @Override
    public void handleAutomatonTransition(
            Automaton.Transition<ProtocolState> transition) {
        // TODO Auto-generated method stub
    }

    @Override
    public void handleNotification(
            Operation.ProtocolResponse<IWatcherEvent> notification) {
        WatchEvent event = WatchEvent.fromRecord(notification.record());
        ZNodePath path = event.getPath();
        if (path.startsWith(root)) {
            new Updater(path);
        }
    }

    protected class Updater implements Runnable {
        protected final ListenableFuture<Optional<V>> future;
        
        public Updater(ZNodePath path) {
            this.future = task().setRoot(path).build();
            Futures.addCallback(future, FetchUntil.this, SameThreadExecutor.getInstance());
            FetchUntil.this.addListener(this, SameThreadExecutor.getInstance());
        }
    
        @Override
        public void run() {
            future.cancel(true);
        }
    }
}