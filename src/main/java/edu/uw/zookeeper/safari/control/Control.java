package edu.uw.zookeeper.safari.control;


import java.util.concurrent.Callable;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.*;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.common.SameThreadExecutor;

public abstract class Control {

    public static <T extends Records.Request, V> RequestUntil<T,V> requestUntil(ControlMaterializerService control, Function<? super Records.Response, Optional<V>> transformer, WatchMatcher matcher, T task) {
        return new RequestUntil<T,V>(control, transformer, matcher, task, SettableFuturePromise.<V>create());
    }

    public static class RequestUntil<T extends Records.Request, V> extends RunnablePromiseTask<T, V> {

        protected final WatchMatcher matcher;
        protected final Function<? super Records.Response, Optional<V>> transformer;
        protected final ControlMaterializerService control;
        protected ListenableFuture<? extends Operation.ProtocolResponse<?>> request;
        protected WatchTask watch;
        
        public RequestUntil(ControlMaterializerService control, Function<? super Records.Response, Optional<V>> transformer, WatchMatcher matcher, T task, Promise<V> promise) {
            super(task, promise);
            this.control = control;
            this.matcher = matcher;
            this.transformer = transformer;
            this.request = null;
            this.watch = null;
        }

        @Override
        public synchronized Optional<V> call() throws Exception {
            if (request == null) {
                request = control.submit(task);
                request.addListener(this, SameThreadExecutor.getInstance());
            } else if (request.isDone()) {
                Operation.ProtocolResponse<?> response = request.get();
                Optional<Operation.Error> error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                if (error.isPresent()) {
                    if (watch == null) {
                        watch = new WatchTask();
                    }
                    request = watch.call();
                    request.addListener(this, SameThreadExecutor.getInstance());
                } else {
                    Optional<V> result = transformer.apply(response.record());
                    if (result.isPresent()) {
                        return result;
                    } else {
                        if (watch == null) {
                            watch = new WatchTask();
                        }
                        request = watch.call();
                        request.addListener(this, SameThreadExecutor.getInstance());
                    }
                }
            }
            return Optional.absent();
        }
    
        protected class WatchTask implements Runnable, Callable<ListenableFuture<? extends Operation.ProtocolResponse<?>>>, WatchMatchListener  {
    
            public WatchTask() {
                control.notifications().subscribe(this);
                addListener(this, SameThreadExecutor.getInstance());
            }
    
            @Override
            public ListenableFuture<? extends Operation.ProtocolResponse<?>> call() {
                return control.submit(((Operations.Requests.WatchBuilder<?, ?>) Operations.Requests.fromRecord(task)).setWatch(true).build());
            }
            
            @Override
            public void run() {
                synchronized (RequestUntil.this) {
                    if (isDone()) {
                        control.notifications().unsubscribe(watch);
                    }
                }
            }
    
            @Override
            public void handleWatchEvent(WatchEvent event) {
                synchronized (RequestUntil.this) {
                    if (! isDone()) {
                        request = null;
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
}
