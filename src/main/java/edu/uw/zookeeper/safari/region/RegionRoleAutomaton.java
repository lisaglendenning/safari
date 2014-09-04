package edu.uw.zookeeper.safari.region;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import com.google.common.base.Optional;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.safari.Identifier;

public final class RegionRoleAutomaton implements Automaton<RegionRole, LeaderEpoch> {

    public static RegionRole getRoleFor(Identifier self, Optional<LeaderEpoch> leader) {
        EnsembleRole role;
        if (!leader.isPresent()) {
            role = EnsembleRole.UNKNOWN;
        } else if (self.equals(leader.get().getLeader())) {
            role = EnsembleRole.LEADING;
        } else {
            role = EnsembleRole.FOLLOWING;
        }
        return new RegionRole(role, leader);
    }

    public static RegionRoleAutomaton forRole(Identifier self, RegionRole role) {
        return new RegionRoleAutomaton(self, role);
    }
    
    public static RegionRoleAutomaton unknown(Identifier self) {
        return new RegionRoleAutomaton(self, RegionRole.unknown());
    }
    
    private final Identifier self;
    private RegionRole state;

    protected RegionRoleAutomaton(Identifier self, RegionRole state) {
        this.self = checkNotNull(self);
        this.state = checkNotNull(state);
    }
    
    @Override
    public RegionRole state() {
        return state;
    }

    /**
     * Not threadsafe.
     */
    @Override
    public Optional<Automaton.Transition<RegionRole>> apply(
            @Nullable LeaderEpoch input) {
        Optional<LeaderEpoch> leader = Optional.fromNullable(input);
        RegionRole next = getRoleFor(self, leader);
        if (!state.equals(next)) {
            RegionRole prevState = state;
            state = next;
            return Optional.of(Automaton.Transition.create(prevState, state));
        }
        return Optional.absent();
    }
}
