package edu.uw.zookeeper.safari.control;

import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class RequestUntil<T extends Records.Request, V> extends PromiseTask<T, V> implements Callable<Optional<V>>, Runnable {

    public static <T extends Records.Request, V> RequestUntil<T,V> create(SchemaClientService<ControlZNode<?>,?> client, Function<? super Records.Response, Optional<V>> transformer, WatchMatcher matcher, T task, Promise<V> promise) {
        RequestUntil<T,V> instance = new RequestUntil<T,V>(client, transformer, matcher, task, promise);
        instance.run();
        return instance;
    }
    
    protected final WatchMatcher matcher;
    protected final Function<? super Records.Response, Optional<V>> transformer;
    protected final SchemaClientService<ControlZNode<?>,?> client;
    protected final CallablePromiseTask<RequestUntil<T,V>,V> delegate;
    protected Optional<? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> request;
    protected Optional<WatchTask> watch;
    
    public RequestUntil(SchemaClientService<ControlZNode<?>,?> client, Function<? super Records.Response, Optional<V>> transformer, WatchMatcher matcher, T task, Promise<V> promise) {
        super(task, promise);
        this.client = client;
        this.matcher = matcher;
        this.transformer = transformer;
        this.delegate = CallablePromiseTask.create(this, this);
        this.request = Optional.absent();
        this.watch = Optional.absent();
    }

    @Override
    public synchronized void run() {
        delegate.run();
    }

    @Override
    public synchronized Optional<V> call() throws Exception {
        if (!request.isPresent()) {
            request = Optional.of(client.materializer().submit(task));
            request.get().addListener(this, SameThreadExecutor.getInstance());
        } else if (request.get().isDone()) {
            Operation.ProtocolResponse<?> response = request.get().get();
            Optional<Operation.Error> error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
            if (error.isPresent()) {
                if (!watch.isPresent()) {
                    watch = Optional.of(new WatchTask());
                }
                request = Optional.of(watch.get().call());
                request.get().addListener(this, SameThreadExecutor.getInstance());
            } else {
                Optional<V> result = transformer.apply(response.record());
                if (result.isPresent()) {
                    return result;
                } else {
                    if (!watch.isPresent()) {
                        watch = Optional.of(new WatchTask());
                    }
                    request = Optional.of(watch.get().call());
                    request.get().addListener(this, SameThreadExecutor.getInstance());
                }
            }
        }
        return Optional.absent();
    }

    protected class WatchTask implements Runnable, Callable<ListenableFuture<? extends Operation.ProtocolResponse<?>>>, WatchMatchListener  {

        public WatchTask() {
            client.notifications().subscribe(this);
            addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public ListenableFuture<? extends Operation.ProtocolResponse<?>> call() {
            return client.materializer().submit(((Operations.Requests.WatchBuilder<?, ?>) Operations.Requests.fromRecord(task)).setWatch(true).build());
        }
        
        @Override
        public void run() {
            synchronized (RequestUntil.this) {
                if (isDone()) {
                    client.notifications().unsubscribe(watch.get());
                }
            }
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            synchronized (RequestUntil.this) {
                if (! isDone()) {
                    request = Optional.absent();
                    run();
                }
            }
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            // TODO
        }

        @Override
        public WatchMatcher getWatchMatcher() {
            return matcher;
        }
    }
}