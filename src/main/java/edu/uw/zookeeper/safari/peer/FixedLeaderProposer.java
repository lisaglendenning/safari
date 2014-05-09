package edu.uw.zookeeper.safari.peer;

import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlZNode;

/**
 * Always proposes the same leader.
 */
public class FixedLeaderProposer implements Callable<ListenableFuture<LeaderEpoch>>, Function<LeaderEpoch, LeaderEpoch> {

    public static FixedLeaderProposer create(
            Identifier peer,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            Materializer<ControlZNode<?>,?> materializer) {
        return new FixedLeaderProposer(peer, region, role, materializer);
    }
    
    protected final Logger logger;
    protected final Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role;
    protected final Identifier region;
    protected final Identifier peer;
    protected final Materializer<ControlZNode<?>,?> materializer;
    
    protected FixedLeaderProposer(
            Identifier peer,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            Materializer<ControlZNode<?>,?> materializer) {
        this.logger = LogManager.getLogger(this);
        this.materializer = materializer;
        this.role = role;
        this.region = region;
        this.peer = peer;
    }
    
    @Override
    public ListenableFuture<LeaderEpoch> call() {
        LeaderEpoch current = role.state().getLeader();
        Optional<Integer> epoch = (current == null) ? Optional.<Integer>absent() : Optional.of(current.getEpoch());
        LeaderProposal proposal = LeaderProposal.create(
                region, peer, epoch, materializer, 
                LoggingPromise.create(logger, SettableFuturePromise.<LeaderEpoch>create()));
        // update role before listeners see it
        return Futures.transform(proposal, this, SameThreadExecutor.getInstance());
    }

    @Override
    public LeaderEpoch apply(LeaderEpoch input) {
        role.apply(input);
        return input;
    }
}