package edu.uw.zookeeper.safari.region;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.AbstractPair;

public final class RegionRole extends AbstractPair<EnsembleRole, Optional<LeaderEpoch>> {

    public static RegionRole unknown() {
        return new RegionRole(EnsembleRole.UNKNOWN, Optional.<LeaderEpoch>absent());
    }
    
    public static RegionRole create(EnsembleRole role, Optional<LeaderEpoch> leader) {
        return new RegionRole(role, leader);
    }
    
    protected RegionRole(EnsembleRole role, Optional<LeaderEpoch> leader) {
        super(checkNotNull(role), checkNotNull(leader));
    }
    
    public boolean hasEpoch() {
        return second.isPresent();
    }
    
    public LeaderEpoch getEpoch() {
        return second.get();
    }
    
    public EnsembleRole getRole() {
        return first;
    }
}
