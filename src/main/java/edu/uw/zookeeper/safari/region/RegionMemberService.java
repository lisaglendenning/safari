package edu.uw.zookeeper.safari.region;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Call;
import edu.uw.zookeeper.common.ForwardingServiceListener;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceMonitor;
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
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.peer.PeerConnectionsService;

public class RegionMemberService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Provides @Singleton
        public Callable<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> getLeaderProposer(
                @Peer Identifier peer, 
                @Region Identifier region,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
                ControlClientService control) {
            return LeaderProposer.fixedLeader(peer, region, role, control.materializer());
        }
        
        @Provides @Singleton
        public RegionMemberService getRegionRoleService(
                @Peer Identifier peer, 
                @Region Identifier region,
                Callable<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
                ControlClientService control,
                PeerConnectionsService connections,
                RegionPlayerService player,
                Injector injector,
                ServiceMonitor monitor) {
            RegionMemberService instance =
                    RegionMemberService.create(
                            peer, region, proposer, role, control,
                            ImmutableList.<Service.Listener>of(
                                    ForwardingServiceListener.forService(connections),
                                    ForwardingServiceListener.forService(player)));
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }
    
    public static RegionMemberService create(
            Identifier peer,
            Identifier region,
            Callable<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            ControlClientService control,
            Iterable<? extends Service.Listener> listeners) {
        RegionMemberService instance = new RegionMemberService(
                ImmutableList.<Service.Listener>builder().addAll(listeners).add(new Advertiser(peer, region, control.materializer())).build());
        RoleCacheListener.create(control.materializer().cache(), region, role, instance, control.cacheEvents());
        newLeaderWatcher(region, instance, control.notifications(), control.materializer());
        newLeaderProposer(proposer, instance, role, control);
        return instance;
    }

    public static <O extends Operation.ProtocolResponse<?>> Watchers.RunnableWatcher<?> newLeaderWatcher(
            Identifier region,
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,O,?> client) {
        final AbsoluteZNodePath path = ControlSchema.Safari.Regions.Region.Leader.pathOf(region);
        final FixedQuery<O> query = FixedQuery.forRequests(
                client,
                Operations.Requests.sync().setPath(path).build(),
                Operations.Requests.getData().setPath(path).setWatch(true).build());
        return Watchers.RunnableWatcher.listen(
                Call.create(query),
                service, 
                watch, 
                WatchMatcher.exact(
                        ControlSchema.Safari.Regions.Region.Leader.pathOf(region), 
                        EnumSet.of(EventType.NodeDeleted, EventType.NodeCreated, EventType.NodeDataChanged)));
    }
    
    public static Service newLeaderProposer(
            final Callable<? extends ListenableFuture<Optional<Automaton.Transition<RegionRole>>>> proposer,
            final Service service,
            final Automatons.EventfulAutomaton<RegionRole,?> role,
            final ControlClientService control) {
        final Runnable propose = new Runnable() {
            @Override
            public void run() {
                try {
                    proposer.call();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
        LeaderPresenceListener.create(propose, role, service, control);
        service.addListener(
                new Service.Listener() {
                    @Override
                    public void running() {
                        propose.run();
                    }
                }, 
                SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            propose.run();
        }
        return service;
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
