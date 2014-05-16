package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.SameThreadExecutor;

public abstract class WaitingActor<T extends ListenableFuture<?>,V> extends Actors.ExecutedPeekingQueuedActor<V> {
    
    protected Optional<? extends T> waiting;
    
    protected WaitingActor(Executor executor, Queue<V> mailbox, Logger logger) {
        super(executor, mailbox, logger);
        this.waiting = Optional.absent();
    }
    
    public synchronized T wait(T future) {
        this.waiting = Optional.of(future);
        future.addListener(this, SameThreadExecutor.getInstance());
        return future;
    }

    @Override
    public synchronized boolean isReady() {
        return !mailbox.isEmpty() && (!waiting.isPresent() || waiting.get().isDone());
    }
    
    @Override
    protected synchronized void doRun() throws Exception {
        if (waiting.isPresent()) {
            assert (waiting.get().isDone());
            waiting.get().get();
            waiting = Optional.absent();
        }
        
        super.doRun();
    }
}