package edu.uw.zookeeper.safari.peer;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Function;
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
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ParameterizedFactory;
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
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.FixedQuery;
import edu.uw.zookeeper.safari.data.RunnableWatcher;

@DependsOn(PeerConnectionsService.class)
public class RegionMemberService extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Provides @Singleton
        public Callable<? extends ListenableFuture<LeaderEpoch>> getLeaderProposer(
                PeerConfiguration peer, 
                RegionConfiguration region,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
                ControlMaterializerService control) {
            return FixedLeaderProposer.create(peer.getView().id(), region.getRegion(), role, control.materializer());
        }

        @Provides @Singleton
        public Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> getRegionRole(
                PeerConfiguration peer) {
            return Automatons.createSynchronizedEventful(
                    Automatons.createEventful(RegionRoleAutomaton.unknown(peer.getView().id())));
        }
        
        @Provides @Singleton
        public RegionMemberService getRegionMemberService(
                PeerConfiguration peer, 
                RegionConfiguration region,
                Callable<? extends ListenableFuture<LeaderEpoch>> proposer,
                Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
                ControlMaterializerService control,
                Injector injector,
                ServiceMonitor monitor) {
            RegionMemberService instance = monitor.addOnStart(
                    RegionMemberService.create(
                            peer.getView().id(), region.getRegion(), proposer, role, control, injector));
            newPlayerRoleListener(instance, control);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }
    
    public static RegionMemberService create(
            Identifier peer,
            Identifier region,
            Callable<? extends ListenableFuture<LeaderEpoch>> proposer,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            ControlMaterializerService control,
            Injector injector) {
        RegionMemberService instance = new RegionMemberService(peer, region, role, control, injector);
        RoleCacheListener.create(instance, control, region, role);
        newLeaderProposer(proposer, instance, role, control);
        newLeaderWatcher(region, instance, control.notifications(), control);
        instance.new Advertiser(SameThreadExecutor.getInstance());
        return instance;
    }

    public static <O extends Operation.ProtocolResponse<?>> RunnableWatcher<?> newLeaderWatcher(
            Identifier region,
            Service service,
            WatchListeners watch,
            ClientExecutor<? super Records.Request,O,?> client) {
        final AbsoluteZNodePath path = ControlSchema.Safari.Regions.Region.Leader.pathOf(region);
        final FixedQuery<O> query = FixedQuery.forRequests(
                client,
                Operations.Requests.sync().setPath(path).build(),
                Operations.Requests.getData().setPath(path).setWatch(true).build());
        return RunnableWatcher.newInstance(
                        service, 
                        watch, 
                        WatchMatcher.exact(ControlSchema.Safari.Regions.Region.Leader.pathOf(region), EnumSet.of(EventType.NodeDeleted, EventType.NodeCreated, EventType.NodeDataChanged)),
                        new Runnable() {
                            @Override
                            public void run() {
                                query.call();
                            }
                        });
    }
    
    public static Service newLeaderProposer(
            final Callable<? extends ListenableFuture<LeaderEpoch>> proposer,
            final Service service,
            final Automatons.EventfulAutomaton<RegionRole,?> role,
            final ControlMaterializerService control) {
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
                new Listener() {
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

    public static PlayerRoleListener newPlayerRoleListener(
            final RegionMemberService member,
            final ControlMaterializerService control) {
        return PlayerRoleListener.create(new ParameterizedFactory<RegionRole, Service>() {
            @Override
            public Service get(RegionRole value) {
                switch (value.getRole()) {
                case LEADING:
                    return RegionLeaderService.create(member.getRegion(), control);
                default:
                    return null;
                }
            }
        }, member.getRole(), member);
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
                                Operations.unlessError(response.record());
                            } catch (KeeperException e) {
                                throw new IllegalStateException(String.format("Error creating member %s for region %s", peer, region), e);
                            }
                        }
                        return null;
                    }
        });
    }
    
    protected final Logger logger;
    protected final ControlMaterializerService control;
    protected final Identifier peer;
    protected final Identifier region;
    protected final Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role;
    
    protected RegionMemberService(
            Identifier peer,
            Identifier region,
            Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role,
            ControlMaterializerService control,
            Injector injector) {
        super(injector);
        this.logger = LogManager.getLogger(this);
        this.control = control;
        this.role = role;
        this.peer = peer;
        this.region = region;
    }
    
    public Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> getRole() {
        return role;
    }
    
    public Identifier getRegion() {
        return region;
    }
    
    public Identifier getPeer() {
        return peer;
    }

    public class Advertiser extends Service.Listener {
    
        public Advertiser(
                Executor executor) {
            addListener(this, executor);
        }
    
        @Override
        public void running() {
            try {
                RegionMemberService.advertise(getPeer(), getRegion(), control.materializer()).get();
            } catch (Exception e) {
                logger.warn("", e);
                injector.getInstance(RegionMemberService.class).stopAsync();
            }
        }
    }
}
