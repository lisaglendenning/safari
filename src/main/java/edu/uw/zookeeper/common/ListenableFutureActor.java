package edu.uw.zookeeper.common;

import java.util.Queue;

import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.TaskExecutor;

public abstract class ListenableFutureActor<I,V,T extends ListenableFuture<V>> extends Actors.PeekingQueuedActor<T> implements TaskExecutor<I,V> {

    protected ListenableFutureActor(
            Queue<T> mailbox,
            Logger logger) {
        super(mailbox, logger);
    }

    @Override
    public boolean isReady() {
        T next = next();
        return ((next != null) && next.isDone());
    }

    @Override
    protected boolean doSend(T message) {
        final boolean sent = super.doSend(message);
        if (sent) {
            message.addListener(this, SameThreadExecutor.getInstance());
        }
        return sent;
    }

    @Override
    protected boolean apply(T input) throws Exception {
        if (input.isDone() && mailbox.remove(input)) {
            return doApply(input);
        }
        return false;
    }

    @Override
    protected void doStop() {
        T next;
        while ((next = mailbox.poll()) != null) {
            next.cancel(false);
        }
    }
    
    protected abstract boolean doApply(T input) throws Exception;
}
