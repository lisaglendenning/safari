package edu.uw.zookeeper.safari.frontend;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.peer.EnsembleConfiguration;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;
import edu.uw.zookeeper.safari.peer.PeerConnectionsService;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;

@DependsOn({PeerToEnsembleLookup.class, PeerConnectionsService.class})
public class RegionsConnectionsService extends AbstractIdleService implements AsyncFunction<Identifier, ClientPeerConnection<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}

        @Provides @Singleton
        public RegionsConnectionsService getEnsembleConnectionsService(
                PeerConfiguration peer,
                EnsembleConfiguration ensemble,
                ControlMaterializerService control,
                ClientPeerConnections peerConnections) {
            return RegionsConnectionsService.defaults(
                            peerConnections.asLookup(),
                            peer.getView().id(),
                            ensemble.getEnsemble(), 
                            control.materializer());
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(PeerToEnsembleLookup.module());
        }
    }

    public static RegionsConnectionsService defaults(
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
            Identifier myId,
            Identifier myRegion,
            Materializer<?> control) {
        AsyncFunction<Identifier, Identifier> selector = 
                SelectSelfTask.defaults(myId, myRegion, control);
        return new RegionsConnectionsService(
                selector,
                connector);
    }

    protected static Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final ConcurrentMap<Identifier, RegionClientPeerConnection> regions;
    protected final AsyncFunction<Identifier, Identifier> selector;
    protected final AsyncFunction<Identifier, ClientPeerConnection<?>> connector;
    
    protected RegionsConnectionsService(
            AsyncFunction<Identifier, Identifier> selector,
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector) {
        this.regions = new MapMaker().makeMap();
        this.selector = selector;
        this.connector = connector;
    }
    
    @Override
    public ListenableFuture<ClientPeerConnection<?>> apply(Identifier region) {
        RegionClientPeerConnection connection = regions.get(region);
        if (connection == null) {
            regions.putIfAbsent(region, RegionClientPeerConnection.newInstance(region, selector, connector));
            connection = regions.get(region);
        }
        return connection;
    }

    @Override
    protected void startUp() throws Exception {
    }
    
    @Override
    protected void shutDown() throws Exception {
    }

    public static class SelectRandom<V> implements Function<List<V>, V> {
        
        public static <V> SelectRandom<V> create() {
            return new SelectRandom<V> ();
        }
        
        private final Random random;
        
        public SelectRandom() {
            this.random = new Random();
        }
    
        @Override
        @Nullable
        public V apply(List<V> input) {
            if (input.isEmpty()) {
                return null;
            } else {
                int index = random.nextInt(input.size());
                return input.get(index);
            }
        }
    }
    
    public static class SelectSelfTask implements AsyncFunction<Identifier, Identifier> {

        public static SelectSelfTask defaults(
                Identifier myId,
                Identifier myRegion,
                Materializer<?> materializer) {
            return new SelectSelfTask(myId, myRegion, 
                    SelectMemberTask.defaults(materializer));
        }
        
        protected final Identifier myId;
        protected final Identifier myRegion;
        protected final AsyncFunction<Identifier, Identifier> fallback;
        
        public SelectSelfTask(
                Identifier myId,
                Identifier myEnsemble,
                AsyncFunction<Identifier, Identifier> fallback) {
            this.myId = myId;
            this.myRegion = myEnsemble;
            this.fallback = fallback;
        }

        @Override
        public ListenableFuture<Identifier> apply(Identifier region)
                throws Exception {
            return region.equals(myRegion) ?
                    Futures.immediateFuture(myId) : 
                        fallback.apply(region);
        }
    }
    
    public static class SelectMemberTask<T> implements AsyncFunction<Identifier, Identifier> {

        public static SelectMemberTask<List<ControlSchema.Regions.Entity.Members.Member>> defaults(Materializer<?> materializer) {
            return new SelectMemberTask<List<ControlSchema.Regions.Entity.Members.Member>>(
                    ControlSchema.Regions.Entity.Members.getMembers(materializer),
                    SelectPresentMemberTask.defaults(materializer));
        }
        
        protected final CachedFunction<Identifier, ? extends T> memberLookup;
        protected final AsyncFunction<? super T, Identifier> selector;
        
        public SelectMemberTask(
                CachedFunction<Identifier, ? extends T> memberLookup,
                AsyncFunction<? super T, Identifier> selector) {
            this.memberLookup = memberLookup;
            this.selector = selector;
        }

        @Override
        public ListenableFuture<Identifier> apply(Identifier ensemble)
                throws Exception {
            // never used cached members
            return Futures.transform(
                    memberLookup.async().apply(ensemble), 
                    selector, 
                    SAME_THREAD_EXECUTOR);
        }
    }

    public static class SelectPresentMemberTask implements AsyncFunction<List<ControlSchema.Regions.Entity.Members.Member>, Identifier> {

        public static SelectPresentMemberTask defaults(
                ClientExecutor<? super Records.Request, ?, ?> client) {
            return new SelectPresentMemberTask(
                    client,
                    SelectRandom.<ControlSchema.Regions.Entity.Members.Member>create());
        }
        
        protected final ClientExecutor<? super Records.Request, ?, ?> client;
        protected final Function<List<ControlSchema.Regions.Entity.Members.Member>, ControlSchema.Regions.Entity.Members.Member> selector;
        
        public SelectPresentMemberTask(
                ClientExecutor<? super Records.Request, ?, ?> client,
                Function<List<ControlSchema.Regions.Entity.Members.Member>, ControlSchema.Regions.Entity.Members.Member> selector) {
            this.client = client;
            this.selector = selector;
        }
        
        @Override
        public ListenableFuture<Identifier> apply(
                List<ControlSchema.Regions.Entity.Members.Member> members) {
            List<ListenableFuture<Boolean>> presence = Lists.newArrayListWithCapacity(members.size());
            for (ControlSchema.Regions.Entity.Members.Member e: members) {
                presence.add(ControlSchema.Peers.Entity.of(e.get()).presence().exists(client));
            }
            ListenableFuture<List<Boolean>> future = Futures.successfulAsList(presence);
            return Futures.transform(future, SelectPresentMemberFunction.defaults(members, selector), SAME_THREAD_EXECUTOR);
        }
    }
    
    public static class SelectPresentMemberFunction implements Function<List<Boolean>, Identifier> {

        public static SelectPresentMemberFunction defaults(
                List<ControlSchema.Regions.Entity.Members.Member> members,
                Function<List<ControlSchema.Regions.Entity.Members.Member>, ControlSchema.Regions.Entity.Members.Member> selector) {
            return new SelectPresentMemberFunction(members, selector);
        }
        
        protected final List<ControlSchema.Regions.Entity.Members.Member> members;
        protected final Function<List<ControlSchema.Regions.Entity.Members.Member>, ControlSchema.Regions.Entity.Members.Member> selector;
        
        public SelectPresentMemberFunction(
                List<ControlSchema.Regions.Entity.Members.Member> members,
                Function<List<ControlSchema.Regions.Entity.Members.Member>, ControlSchema.Regions.Entity.Members.Member> selector) {
            this.members = members;
            this.selector = selector;
        }
        
        @Override
        public Identifier apply(List<Boolean> presence) {
            List<ControlSchema.Regions.Entity.Members.Member> living = Lists.newArrayListWithCapacity(members.size());
            for (int i=0; i<members.size(); ++i) {
                try {
                    if (Boolean.TRUE.equals(presence.get(i))) {
                        living.add(members.get(i));
                    }
                } catch (Exception e) {}
            }
            ControlSchema.Regions.Entity.Members.Member selected = selector.apply(living);
            return (selected == null) ? null : selected.get();
        }
    }
}
