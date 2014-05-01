package edu.uw.zookeeper.safari.peer;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.safari.Identifier;

public class RegionRoleAutomaton implements Automaton<RegionRole, LeaderEpoch> {

    public static RegionRoleAutomaton unknown(Identifier self) {
        return new RegionRoleAutomaton(self, null);
    }
    
    private Identifier self;
    private RegionRole state;

    public RegionRoleAutomaton(Identifier self, @Nullable LeaderEpoch leader) {
        this.self = checkNotNull(self);
        this.state = new RegionRole(leader, getRoleFor(leader));
    }
    
    @Override
    public RegionRole state() {
        return state;
    }

    @Override
    public Optional<Automaton.Transition<RegionRole>> apply(
            @Nullable LeaderEpoch input) {
        if (! Objects.equal(state.getLeader(), input) 
                && ((state.getLeader() == null) 
                        || (input == null) 
                        || (state.getLeader().getEpoch() < input.getEpoch()))) {
            RegionRole prevState = state;
            state = new RegionRole(input, getRoleFor(input));
            return Optional.of(Automaton.Transition.create(prevState, state));
        } else {
            return Optional.absent();
        }
    }

    public EnsembleRole getRoleFor(LeaderEpoch leader) {
        if ((leader == null) || (leader.getLeader() == null)) {
            return EnsembleRole.LOOKING;
        } else if (self.equals(leader.getLeader())) {
            return EnsembleRole.LEADING;
        } else {
            return EnsembleRole.FOLLOWING;
        }
    }
}
