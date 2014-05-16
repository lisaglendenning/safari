package edu.uw.zookeeper.safari.region;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Objects;

import edu.uw.zookeeper.safari.Identifier;

public final class LeaderEpoch {

    public static LeaderEpoch of(int epoch, Identifier leader) {
        return new LeaderEpoch(epoch, leader);
    }

    private final int epoch;
    private final Identifier leader;
    
    public LeaderEpoch(int epoch, Identifier leader) {
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
        return Objects.toStringHelper(this).add("epoch", epoch).add("leader", leader).toString();
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
