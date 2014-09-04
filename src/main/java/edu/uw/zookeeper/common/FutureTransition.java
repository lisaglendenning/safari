package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractPair;

public final class FutureTransition<V> extends AbstractPair<Optional<V>, ListenableFuture<V>> {

    public static <V> FutureTransition<V> absent(ListenableFuture<V> next) {
        return new FutureTransition<V>(Optional.<V>absent(), next);
    }
    
    public static <V> FutureTransition<V> present(V current,
            ListenableFuture<V> next) {
        return new FutureTransition<V>(Optional.of(current), checkNotNull(next));
    }
    
    protected FutureTransition(Optional<V> current,
            ListenableFuture<V> next) {
        super(current, next);
    }

    public Optional<V> getCurrent() {
        return first;
    }
    
    public ListenableFuture<V> getNext() {
        return second;
    }
}
