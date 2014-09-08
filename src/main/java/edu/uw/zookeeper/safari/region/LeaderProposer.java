package edu.uw.zookeeper.safari.region;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.LoggingFutureListener;

import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class LeaderProposer implements Supplier<ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> {

    /**
     * Always proposes the same leader.
     */
    public static LeaderProposer fixedLeader(
            final Identifier peer,
            final Identifier region,
            final Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            final Materializer<ControlZNode<?>,?> materializer) {
        return create(new Supplier<Identifier>() {
            @Override
            public Identifier get() {
                return peer;
            }
        }, region, role, materializer);
    }
    
    public static LeaderProposer create(
            Supplier<Identifier> leader,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            Materializer<ControlZNode<?>,?> materializer) {
        return new LeaderProposer(leader, region, role, materializer);
    }
    
    protected final Logger logger;
    protected final Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role;
    protected final Identifier region;
    protected final Supplier<Identifier> leader;
    protected final Materializer<ControlZNode<?>,?> materializer;
    
    protected LeaderProposer(
            Supplier<Identifier> leader,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            Materializer<ControlZNode<?>,?> materializer) {
        this.logger = LogManager.getLogger(this);
        this.materializer = materializer;
        this.role = role;
        this.region = region;
        this.leader = leader;
    }
    
    @Override
    public ListenableFuture<Optional<Automaton.Transition<RegionRole>>> get() {
        Optional<Integer> epoch = role.state().hasEpoch() ? Optional.of(role.state().getEpoch().getEpoch()) : Optional.<Integer>absent();
        ListenableFuture<LeaderEpoch> proposal = 
                LoggingFutureListener.listen(logger, LeaderProposal.call(
                region, leader.get(), epoch, materializer));
        // update role before listeners see it
        return Futures.transform(proposal, role);
    }
}
