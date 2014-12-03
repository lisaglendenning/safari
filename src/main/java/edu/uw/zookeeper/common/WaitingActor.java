package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actors;


public abstract class WaitingActor<T extends ListenableFuture<?>,V> extends Actors.ExecutedPeekingQueuedActor<V> {
    
    protected Optional<? extends T> waiting;
    
    protected WaitingActor(Executor executor, Queue<V> mailbox, Logger logger) {
        super(executor, mailbox, logger);
        this.waiting = Optional.absent();
    }
    
    public synchronized T wait(T future) {
        this.waiting = Optional.of(future);
        future.addListener(this, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public synchronized boolean isReady() {
        return !mailbox.isEmpty() && (!waiting.isPresent() || waiting.get().isDone());
    }
    
    @Override
    protected synchronized void doRun() throws Exception {
        clearWaiting();
        super.doRun();
    }
    
    public synchronized Optional<?> clearWaiting() throws Exception {
        Optional<?> result = Optional.absent();
        if (waiting.isPresent()) {
            assert (waiting.get().isDone());
            result = Optional.of(waiting.get().get());
            waiting = Optional.absent();
        }
        return result;
    }
}
