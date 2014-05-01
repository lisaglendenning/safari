package edu.uw.zookeeper.safari.peer;

import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;


public class RegionRoleService extends AbstractIdleService {

    public static RegionRoleService newInstance(
            Identifier peer, 
            Identifier region,
            ControlMaterializerService control) {
        return new RegionRoleService(peer, region, control);
    }
    
    protected final ControlMaterializerService control;
    protected final Identifier region;
    protected final Identifier peer;
    protected final Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role;
    protected final AbsoluteZNodePath leaderPath;
    protected final LeaderProposer proposer;
    protected final LeaderWatcher watcher;
    protected final PresenceListener presence;
    
    protected RegionRoleService(
            Identifier peer, 
            Identifier region,
            ControlMaterializerService control) {
        this.control = control;
        this.role = Automatons.createSynchronizedEventful(
                Automatons.createEventful(RegionRoleAutomaton.unknown(peer)));
        this.peer = peer;
        this.region = region;
        this.leaderPath = ControlSchema.Safari.Regions.Region.Leader.pathOf(region);
        this.proposer = new LeaderProposer();
        this.watcher = new LeaderWatcher();
        this.presence = new PresenceListener();
    }
    
    public Identifier getRegion() {
        return region;
    }
    
    public Identifier getPeer() {
        return peer;
    }

    public Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> getRole() {
        return role;
    }
    
    public LeaderProposer getProposer() {
        return proposer;
    }

    @Override
    protected void startUp() throws Exception {
        proposer.call();
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    /**
     * Always proposes the same peer.
     */
    protected class LeaderProposer extends ForwardingListenableFuture<LeaderEpoch> implements Callable<ListenableFuture<LeaderEpoch>>, Runnable {
    
        protected Promise<LeaderEpoch> promise;
        protected LeaderProposal proposal;
        
        public LeaderProposer() {
            this.promise = SettableFuturePromise.create();
            this.proposal = null;
        }
        
        @Override
        public synchronized ListenableFuture<LeaderEpoch> call() {
            if (promise.isDone()) {
                promise = SettableFuturePromise.create();
                proposal = null;
            }
            run();
            return promise;
        }

        @Override
        public synchronized void run() {
            if (! promise.isDone()) {
                if (proposal == null) {
                    LeaderEpoch current = getRole().state().getLeader();
                    Optional<Integer> epoch = (current == null) ? Optional.<Integer>absent() : Optional.of(current.getEpoch());
                    proposal = LeaderProposal.newInstance(region, peer, epoch, control.materializer());
                    proposal.addListener(this, SameThreadExecutor.getInstance());
                    proposal.run();
                } else if (proposal.isDone()) {
                    // update our state before listeners see it
                    try {
                        LeaderEpoch result = proposal.get();
                        assert (result != null);
                        getRole().apply(result);
                        promise.set(result);
                    } catch (InterruptedException e) {
                        throw new AssertionError();
                    } catch (ExecutionException e) {
                        promise.setException(e);
                    }
                }
            }
        }
        
        @Override
        protected synchronized ListenableFuture<LeaderEpoch> delegate() {
            return promise;
        }
    }
    
    protected class PresenceListener extends Service.Listener implements Runnable, Automatons.AutomatonListener<RegionRole> {

        protected PresenceWatcher presence;
        
        public PresenceListener() {
            this.presence = null;
            
            RegionRoleService.this.addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public void starting() {
            role.subscribe(this);
        }

        @Override
        public void stopping(State from) {
            role.unsubscribe(this);
        }

        @Override
        public synchronized void run() {
            if (isRunning()) {
                if ((presence != null) && presence.isDone()) {
                    presence = null;
                    proposer.call();
                }
            }
        }

        @Override
        public synchronized void handleAutomatonTransition(
                Automaton.Transition<RegionRole> transition) {
            switch (transition.to().getRole()) {
            case FOLLOWING:
            {
                presence = new PresenceWatcher(transition.to().getLeader().getLeader());
                presence.addListener(this, SameThreadExecutor.getInstance());
                break;
            }
            default:
            {
                presence = null;
                break;
            }
            }
        }
    }

    protected class PresenceWatcher extends ForwardingListenableFuture<Identifier> implements WatchMatchListener, Runnable {

        protected final Promise<Identifier> promise;
        protected final AbsoluteZNodePath presence;
        protected final WatchMatcher matcher;
        protected ListenableFuture<? extends Operation.ProtocolResponse<?>> request;
        
        public PresenceWatcher(Identifier peer) {
            this.promise = SettableFuturePromise.create();
            this.presence = ControlSchema.Safari.Peers.Peer.Presence.pathOf(peer);
            this.matcher = WatchMatcher.exact(presence, EnumSet.of(EventType.NodeDeleted));
            this.request = null;
            
            this.promise.addListener(this, SameThreadExecutor.getInstance());
        }
        
        public Identifier getPeer() {
            return Identifier.valueOf(((AbsoluteZNodePath) presence.parent()).label().toString());
        }

        @Override
        public synchronized void run() {
            if (! isDone()) {
                if (request == null) {
                    control.notifications().subscribe(this);
                    control.submit(Operations.Requests.sync().setPath(presence).build());
                    request = control.submit(Operations.Requests.exists().setPath(presence).setWatch(true).build());
                    request.addListener(this, SameThreadExecutor.getInstance());
                } else if (request.isDone()) {
                    try {
                        Optional<Operation.Error> error = Operations.maybeError(request.get().record(), KeeperException.Code.NONODE);
                        if (error.isPresent()) {
                            promise.set(getPeer());
                        }
                    } catch (Exception e) {
                        // TODO
                        promise.setException(e);
                    }
                }
            } else {
                control.notifications().unsubscribe(this);
            }
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            switch (event.getEventType()) {
            case NodeDeleted:
            {
                promise.set(getPeer());
                break;
            }
            default:
                throw new AssertionError(String.valueOf(event));
            }
        }

        @Override
        public WatchMatcher getWatchMatcher() {
            return matcher;
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }

        @Override
        protected ListenableFuture<Identifier> delegate() {
            return promise;
        }
    }

    protected class LeaderWatcher extends Service.Listener implements Runnable {

        protected final WatchMatcher matcher;
        protected final LeaderNotificationWatcher notificationWatcher;
        protected final LeaderCacheWatcher cacheWatcher;
        
        public LeaderWatcher() {
            this.matcher = WatchMatcher.exact(ControlSchema.Safari.Regions.Region.Leader.pathOf(region), EnumSet.of(EventType.NodeDeleted, EventType.NodeCreated, EventType.NodeDataChanged));
            this.notificationWatcher = new LeaderNotificationWatcher();
            this.cacheWatcher = new LeaderCacheWatcher();
            RegionRoleService.this.addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public void starting() {
            notificationWatcher.starting();
            cacheWatcher.starting();
        }

        @Override
        public void running() {
            proposer.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void stopping(State from) {
            notificationWatcher.stopping(from);
            cacheWatcher.stopping(from);
        }

        @Override
        public void run() {
            if (isRunning()) {
                control.submit(Operations.Requests.sync().setPath(leaderPath).build());
                control.submit(Operations.Requests.getData().setPath(leaderPath).setWatch(true).build());
            }
        }

        protected class LeaderNotificationWatcher extends Service.Listener implements WatchMatchListener {

            @Override
            public void starting() {
                control.notifications().subscribe(this);
            }

            @Override
            public void stopping(State from) {
                control.notifications().unsubscribe(this);
            }

            @Override
            public WatchMatcher getWatchMatcher() {
                return matcher;
            }

            @Override
            public void handleAutomatonTransition(
                    Automaton.Transition<ProtocolState> transition) {
                // TODO Auto-generated method stub
            }

            @Override
            public void handleWatchEvent(WatchEvent event) {
                switch (event.getEventType()) {
                case NodeDataChanged:
                case NodeCreated:
                case NodeDeleted:
                {
                    run();
                    break;
                }
                default:
                    throw new AssertionError(String.valueOf(event));
                }
            }
        }
        
        protected class LeaderCacheWatcher extends Service.Listener implements WatchMatchListener {
            @Override
            public void starting() {
                control.cacheEvents().subscribe(this);
            }

            @Override
            public void stopping(State from) {
                control.cacheEvents().unsubscribe(this);
            }

            @Override
            public WatchMatcher getWatchMatcher() {
                return matcher;
            }

            @Override
            public void handleAutomatonTransition(
                    Automaton.Transition<ProtocolState> transition) {
                // TODO Auto-generated method stub
            }

            @Override
            public void handleWatchEvent(WatchEvent event) {
                switch (event.getEventType()) {
                case NodeDataChanged:
                {
                    LeaderEpoch leader;
                    control.materializer().cache().lock().readLock().unlock();
                    try {
                        leader = LeaderEpoch.fromMaterializer().apply(control.materializer().cache().cache().get(ControlSchema.Safari.Regions.Region.Leader.pathOf(region)));
                    } finally {
                        control.materializer().cache().lock().readLock().lock();
                    }
                    if (leader != null) {
                        getRole().apply(leader);
                    }
                    run();
                    break;
                }
                case NodeCreated:
                case NodeDeleted:
                {
                    getRole().apply(null);
                    run();
                    break;
                }
                default:
                    throw new AssertionError(String.valueOf(event));
                }
            }
        }
    }
}
