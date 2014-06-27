package edu.uw.zookeeper.safari;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractPair;

public final class VersionTransition extends AbstractPair<Optional<UnsignedLong>, ListenableFuture<UnsignedLong>> {

    public static VersionTransition absent(ListenableFuture<UnsignedLong> next) {
        return new VersionTransition(Optional.<UnsignedLong>absent(), next);
    }
    
    public static VersionTransition present(UnsignedLong current,
            ListenableFuture<UnsignedLong> next) {
        return new VersionTransition(Optional.of(current), checkNotNull(next));
    }
    
    protected VersionTransition(Optional<UnsignedLong> current,
            ListenableFuture<UnsignedLong> next) {
        super(current, next);
    }

    public Optional<UnsignedLong> getCurrent() {
        return first;
    }
    
    public ListenableFuture<UnsignedLong> getNext() {
        return second;
    }
}
