package edu.uw.zookeeper.safari.region;

import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automaton.Transition;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.peer.PeerConnectionsService;
import edu.uw.zookeeper.safari.peer.PresenceExpired;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class RegionMemberService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}

        @Override
        public Key<? extends Service> getKey() {
            return Key.get(RegionMemberService.class);
        }
        
        @Provides @Singleton
        public LeaderProposer newLeaderProposer(
                @Peer Identifier peer, 
                @Region Identifier region,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
                @Control Materializer<ControlZNode<?>,?> control) {
            return LeaderProposer.fixedLeader(peer, region, role, control);
        }
        
        @Provides @Singleton
        public RegionMemberService newRegionMemberService(
                @Peer Identifier peer, 
                @Region Identifier region,
                Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> automaton,
                Supplier<FutureTransition<RegionRoleService>> role,
                SchemaClientService<ControlZNode<?>,?> control,
                final PeerConnectionsService connections,
                ServiceMonitor monitor) {
            RegionMemberService instance =
                    RegionMemberService.create(
                            peer, 
                            region, 
                            proposer, 
                            role, 
                            automaton, 
                            control,
                            ImmutableList.<Service.Listener>of(
                                    new Service.Listener() {
                                        @Override
                                        public void starting() {
                                            Services.startAndWait(connections);
                                        }
                                    }));
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
            bind(new TypeLiteral<Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>>>() {}).to(LeaderProposer.class);
        }
    }
    
    public static RegionMemberService create(
            Identifier peer,
            Identifier region,
            Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
            Supplier<FutureTransition<RegionRoleService>> role,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> automaton,
            SchemaClientService<ControlZNode<?>,?> control,
            Iterable<? extends Service.Listener> listeners) {
        RegionMemberService instance = new RegionMemberService(
                ImmutableList.<Service.Listener>builder()
                .addAll(listeners)
                .add(new Advertiser(peer, region, control.materializer()))
                .build());
        RoleCacheListener.listen(
                control.materializer().cache(), 
                region, 
                automaton, 
                instance, 
                control.cacheEvents());
        newLeaderWatcher(
                region, 
                instance, 
                control.notifications(), 
                control.materializer());
        RegionRoleListener.listen(
                proposer, 
                role, 
                control.materializer(), 
                control.notifications(), 
                instance, 
                instance.logger());
        return instance;
    }

    public static <O extends Operation.ProtocolResponse<?>> Watchers.RunnableServiceListener<?> newLeaderWatcher(
            Identifier region,
            Service service,
            WatchListeners notifications,
            ClientExecutor<? super Records.Request,O,?> client) {
        final AbsoluteZNodePath path = ControlSchema.Safari.Regions.Region.Leader.pathOf(region);
        @SuppressWarnings("unchecked")
        final Watchers.FixedQueryRunnable<O,?> query = 
                Watchers.FixedQueryRunnable.create(
                        FixedQuery.forIterable(
                            client,
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(),
                                    Operations.Requests.getData().setWatch(true)).apply(path)),
                        Watchers.MaybeErrorProcessor.maybeNoNode(),
                        Watchers.StopServiceOnFailure.create(service));
        return Watchers.RunnableServiceListener.listen(
                query,
                service, 
                notifications, 
                WatchMatcher.exact(
                        path, 
                        EventType.NodeDeleted, 
                        EventType.NodeCreated, 
                        EventType.NodeDataChanged));
    }
    
    // Assumes that the region already exists
    public static ListenableFuture<Void> advertise(
            final Identifier peer, 
            final Identifier region,
            final Materializer<ControlZNode<?>,?> materializer) {
        ZNodePath path = ControlSchema.Safari.Regions.PATH.join(ZNodeLabel.fromString(region.toString())).join(ControlSchema.Safari.Regions.Region.Members.LABEL);
        ImmutableList.Builder<ListenableFuture<? extends Operation.ProtocolResponse<?>>> futures = ImmutableList.builder();
        futures.add(materializer.create(path).call())
                .add(materializer.create(path.join(ZNodeLabel.fromString(peer.toString()))).call());
        return Futures.transform(
                Futures.allAsList(futures.build()), 
                new Function<List<Operation.ProtocolResponse<?>>, Void>() {
                    @Override
                    public Void apply(
                            List<Operation.ProtocolResponse<?>> input) {
                        for (Operation.ProtocolResponse<?> response: input) {
                            try {
                                Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                            } catch (KeeperException e) {
                                throw new IllegalStateException(String.format("Error creating member %s for region %s", peer, region), e);
                            }
                        }
                        return null;
                    }
        });
    }
    
    protected RegionMemberService(
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
    }
    
    protected static final class RegionRoleListener extends AbstractRoleListener<Service> implements Runnable, FutureCallback<RegionRoleService> {

        public static RegionRoleListener listen(
                Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
                Supplier<FutureTransition<RegionRoleService>> role,
                Materializer<ControlZNode<?>,?> materializer,
                WatchListeners notifications,
                Service service,
                Logger logger) {
            RegionRoleListener instance = new RegionRoleListener(proposer, role, materializer, notifications, service, logger);
            Services.listen(instance, service);
            return instance;
        }
        
        private final Function<Identifier, ListenableFuture<Identifier>> expired;
        private final Propose propose;
        
        public RegionRoleListener(
                Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
                Supplier<FutureTransition<RegionRoleService>> role,
                Materializer<ControlZNode<?>,?> materializer,
                WatchListeners notifications,
                Service service,
                Logger logger) {
            super(role, service, logger);
            this.propose = new Propose(proposer, this);
            this.expired = new Expired(materializer, notifications, logger);
        }
        
        @Override
        public void onSuccess(RegionRoleService result) {
            if (logger.isInfoEnabled()) {
                if (result.getRole().hasEpoch()) {
                    logger.info("{} for {}", result.getRole().getRole(), result.getRole().getEpoch());
                } else {
                    logger.info("{}", result.getRole().getRole());
                }
            }
            switch (result.getRole().getRole()) {
            case FOLLOWING:
            {
                Services.listen(new ExpiredListener(expired.apply(result.getRole().getEpoch().getLeader()), result), result);
                break;
            }
            case UNKNOWN:
            {
                Services.listen(new ProposeListener(result), result);
                break;
            }
            default:
                break;
            }
        }

        @Override
        public void onFailure(Throwable t) {
            super.onFailure(t);
            Services.stop(delegate());
        }
        
        protected static final class Expired implements Function<Identifier, ListenableFuture<Identifier>> {
        
            private final Materializer<ControlZNode<?>,?> materializer;
            private final WatchListeners notifications;
            private final Logger logger;
            
            public Expired(
                    Materializer<ControlZNode<?>,?> materializer,
                    WatchListeners notifications,
                    Logger logger) {
                this.materializer = materializer;
                this.notifications = notifications;
                this.logger = logger;
            }
            
            @Override
            public ListenableFuture<Identifier> apply(Identifier input) {
                return LoggingFutureListener.listen(
                        logger, 
                        PresenceExpired.create(
                            input, 
                            materializer,
                            notifications,
                            logger));
            }
        }

        protected static final class Propose implements Runnable, FutureCallback<Optional<Automaton.Transition<RegionRole>>> {

            private final FutureCallback<?> callback;
            private final Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer;
            
            public Propose(
                    Supplier<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
                    FutureCallback<?> callback) {
                this.callback = callback;
                this.proposer = proposer;
            }
            
            @Override
            public void onSuccess(Optional<Transition<RegionRole>> result) {
            }

            @Override
            public void onFailure(Throwable t) {
                callback.onFailure(t);
            }

            @Override
            public void run() {
                Futures.addCallback(proposer.get(), this);
            }
        }
        
        protected class PlayerListener  extends LoggingServiceListener<RegionRoleService> {
            
            protected PlayerListener(
                    RegionRoleService service) {
                super(service);
            }
            
            @Override
            public void failed(Service.State from, Throwable failure) {
                super.failed(from, failure);
                onFailure(failure);
            }
        }

        protected final class ProposeListener extends PlayerListener {
            
            public ProposeListener(
                    RegionRoleService service) {
                super(service);
            }
            
            @Override
            public void running() {
                super.running();
                propose.run();
            }
        }
        
        protected final class ExpiredListener extends PlayerListener implements Runnable {
            
            private final ListenableFuture<Identifier> future;
            
            public ExpiredListener(
                    ListenableFuture<Identifier> future,
                    RegionRoleService service) {
                super(service);
                this.future = future;
            }

            @Override
            public void run() {
                if (future.isDone()) {
                    Identifier result;
                    try {
                        result = future.get();
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (Exception e) {
                        onFailure(e);
                        return;
                    }
                    logger.info("EXPIRED {}", result);
                    propose.run();
                }
            }
            
            @Override
            public void running() {
                super.running();
                future.addListener(this, MoreExecutors.directExecutor());
            }
            
            @Override
            public void failed(Service.State from, Throwable failure) {
                super.failed(from, failure);
                if (!future.isDone()) {
                    future.cancel(false);
                }
            }
        }
    }
    
    protected static class Advertiser extends Service.Listener {

        protected final Identifier peer;
        protected final Identifier region;
        protected final Materializer<ControlZNode<?>,?> control;
        
        public Advertiser(Identifier peer, Identifier region, Materializer<ControlZNode<?>,?> control) {
            this.peer = peer;
            this.region = region;
            this.control = control;
        }
        
        @Override
        public void running() {
            try {
                advertise(peer, region, control).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
