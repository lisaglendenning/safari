package edu.uw.zookeeper.safari.region;

import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlZNode;

public class LeaderProposer implements Callable<ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> {

    /**
     * Always proposes the same leader.
     */
    public static LeaderProposer fixedLeader(
            final Identifier peer,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            Materializer<ControlZNode<?>,?> materializer) {
        return create(new Callable<Identifier>() {
            @Override
            public Identifier call() {
                return peer;
            }
        }, region, role, materializer);
    }
    
    public static LeaderProposer create(
            Callable<Identifier> leader,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            Materializer<ControlZNode<?>,?> materializer) {
        return new LeaderProposer(leader, region, role, materializer);
    }
    
    protected final Logger logger;
    protected final Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role;
    protected final Identifier region;
    protected final Callable<Identifier> leader;
    protected final Materializer<ControlZNode<?>,?> materializer;
    
    protected LeaderProposer(
            Callable<Identifier> leader,
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
    public ListenableFuture<Optional<Automaton.Transition<RegionRole>>> call() throws Exception {
        LeaderEpoch current = role.state().getLeader();
        Optional<Integer> epoch = (current == null) ? Optional.<Integer>absent() : Optional.of(current.getEpoch());
        LeaderProposal proposal = LeaderProposal.create(
                region, leader.call(), epoch, materializer, 
                LoggingPromise.create(logger, SettableFuturePromise.<LeaderEpoch>create()));
        // update role before listeners see it
        return Futures.transform(proposal, role, SameThreadExecutor.getInstance());
    }
}