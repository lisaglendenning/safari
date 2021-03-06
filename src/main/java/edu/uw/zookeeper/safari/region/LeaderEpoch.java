package edu.uw.zookeeper.safari.region;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Objects;

import edu.uw.zookeeper.safari.Identifier;

public final class LeaderEpoch {

    public static LeaderEpoch valueOf(int epoch, Identifier leader) {
        return new LeaderEpoch(epoch, leader);
    }

    private final int epoch;
    private final Identifier leader;
    
    protected LeaderEpoch(int epoch, Identifier leader) {
        super();
        checkArgument(epoch >= 0);
        this.epoch = epoch;
        this.leader = checkNotNull(leader);
    }
    
    public int getEpoch() {
        return epoch;
    }
    
    public Identifier getLeader() {
        return leader;
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append('(').append(epoch).append(',').append(leader).append(')').toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (! (obj instanceof LeaderEpoch)) {
            return false;
        }
        
        LeaderEpoch other = (LeaderEpoch) obj;
        return (epoch == other.epoch) && leader.equals(other.leader);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(epoch, leader);
    }
}
