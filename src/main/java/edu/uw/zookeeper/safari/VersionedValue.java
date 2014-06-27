package edu.uw.zookeeper.safari;

import static com.google.common.base.Preconditions.*;

import javax.annotation.Nullable;

import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.common.AbstractPair;

public class VersionedValue<V> extends AbstractPair<UnsignedLong, V> {

    public static <V> VersionedValue<V> valueOf(
            UnsignedLong version,
            @Nullable V value) {
        return new VersionedValue<V>(version, value);
    }
    
    protected VersionedValue(
            UnsignedLong version,
            @Nullable V value) {
        super(checkNotNull(version), value);
    }
    
    public final UnsignedLong getVersion() {
        return first;
    }

    public final V getValue() {
        return second;
    }
}
