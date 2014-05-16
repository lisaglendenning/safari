package edu.uw.zookeeper.safari.region;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.AbstractPair;

public final class RegionRole extends AbstractPair<LeaderEpoch, EnsembleRole> {

    public RegionRole(LeaderEpoch leader, EnsembleRole role) {
        super(leader, role);
    }
    
    public LeaderEpoch getLeader() {
        return first;
    }
    
    public EnsembleRole getRole() {
        return second;
    }
}
