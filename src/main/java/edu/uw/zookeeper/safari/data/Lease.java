package edu.uw.zookeeper.safari.data;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;


public final class Lease {
    
    public static Lease valueOf(long start, long duration) {
        return new Lease(start, duration);
    }
    
    public static long now() {
        return System.currentTimeMillis();
    }

    private final long start;
    private final long duration;

    protected Lease(
            long start,
            long duration) {
        assert (start >= 0);
        assert (duration >= 0);
        this.start = start;
        this.duration = duration;
    }

    public long getStart() {
        return start;
    }

    public long getDuration() {
        return duration;
    }
    
    public long getEnd() {
        return start + duration;
    }
    
    public long getRemaining() {
        return getEnd() - now();
    }
    
    public TimeUnit getUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("start", start).add("duration", duration).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof Lease)) {
            return false;
        }
        Lease other = (Lease) obj;
        return (start == other.start) && (duration == other.duration);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(start, duration);
    }
}
