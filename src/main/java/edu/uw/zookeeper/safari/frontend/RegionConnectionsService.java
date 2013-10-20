package edu.uw.zookeeper.safari.frontend;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.References;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.peer.EnsembleConfiguration;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;
import edu.uw.zookeeper.safari.peer.PeerConnectionsService;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;

@DependsOn({PeerToEnsembleLookup.class, PeerConnectionsService.class})
public class RegionConnectionsService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}

        @Provides @Singleton
        public RegionConnectionsService getEnsembleConnectionsService(
                PeerConfiguration peer,
                EnsembleConfiguration ensemble,
                ControlMaterializerService control,
                PeerToEnsembleLookup peerToEnsemble,
                ClientPeerConnections peerConnections) {
            return RegionConnectionsService.newInstance(
                            peer.getView().id(),
                            ensemble.getEnsemble(),
                            peerToEnsemble.get().asLookup().cached(),
                            peerConnections, 
                            control);
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(PeerToEnsembleLookup.module());
        }
    }

    public static RegionConnectionsService newInstance(
            Identifier myId,
            Identifier myRegion,
            Function<? super Identifier, Identifier> peerToEnsemble,
            ClientPeerConnections peerConnections,
            ControlMaterializerService control) {
        CachedLookup<Identifier, Identifier> selectedPeers = 
                CachedLookup.create(
                        SelectSelfTask.create(
                                myId, myRegion, control.materializer()));
        return new RegionConnectionsService(
                selectedPeers,
                peerToEnsemble,
                peerConnections,
                control);
    }

    protected static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
    
    protected final ControlMaterializerService control;
    protected final ClientPeerConnections peerConnections;
    protected final CachedLookup<Identifier, Identifier> selectedPeers;
    protected final RegionConnections ensembleConnections;
    protected final Function<? super Identifier, Identifier> peerToEnsemble;
    
    protected RegionConnectionsService(
            CachedLookup<Identifier, Identifier> selectedPeers,
            Function<? super Identifier, Identifier> peerToEnsemble,
            ClientPeerConnections peerConnections,
            ControlMaterializerService control) {
        this.control = control;
        this.peerToEnsemble = peerToEnsemble;
        this.peerConnections = peerConnections;
        this.selectedPeers = selectedPeers;
        this.ensembleConnections = RegionConnections.create(
                selectedPeers.asLookup(), peerConnections.asLookup());
    }

    public CachedLookup<Identifier, Identifier> getPeerForEnsemble() {
        return selectedPeers;
    }

    public CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> getConnectionForEnsemble() {
        return ensembleConnections;
    }

    @Handler
    public void handleClientPeerConnection(ClientPeerConnection<?> connection) {
        new UnselectOnClose(connection);
    }

    @Override
    protected void startUp() throws Exception {
        peerConnections.subscribe(this);
        for (ClientPeerConnection<?> c: peerConnections) {
            handleClientPeerConnection(c);
        }
    }
    
    @Override
    protected void shutDown() throws Exception {
    }
    
    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected class UnselectOnClose {
        protected final ClientPeerConnection<?> connection;
        
        public UnselectOnClose(ClientPeerConnection<?> connection) {
            this.connection = connection;
            connection.subscribe(this);
        }

        @Handler
        public void handleTransition(Automaton.Transition<?> event) {
            if ((event.to() == Connection.State.CONNECTION_CLOSING) ||
                    (event.to() == Connection.State.CONNECTION_CLOSED)) {
                try {
                    connection.unsubscribe(this);
                } catch (Exception e) {}
                
                Identifier peer = connection.remoteAddress().getIdentifier();
                Identifier ensemble = peerToEnsemble.apply(peer);
                if (ensemble == null) {
                    for (Map.Entry<Identifier, Identifier> e: selectedPeers.asCache().entrySet()) {
                        if (e.getValue().equals(peer)) {
                            ensemble = e.getKey();
                            break;
                        }
                    }
                }
                if (ensemble != null) {
                    selectedPeers.asCache().remove(ensemble, peer);
                }
            }
        }
    }

    public static class RegionConnections extends CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> {
    
        public static RegionConnections create(
                final CachedFunction<Identifier, Identifier> peers,
                final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> connectFunction) {
            return new RegionConnections(
                    new Function<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>>() {
                        @Override
                        public @Nullable
                        ClientPeerConnection<Connection<? super MessagePacket<?>>> apply(Identifier ensemble) {
                            Identifier peer = peers.cached().apply(ensemble);
                            if (peer != null) {
                                return connectFunction.cached().apply(peer);
                            } else {
                                return null;
                            }
                        }
                    }, 
                    SharedLookup.<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>>create(
                            new AsyncFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>>() {
                                @Override
                                public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket<?>>>> apply(
                                        Identifier ensemble) throws Exception {
                                    return Futures.transform(
                                            peers.apply(ensemble),
                                            connectFunction,
                                            sameThreadExecutor);
                                }
                            }));
        }
        
        protected RegionConnections(
                Function<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> first,
                AsyncFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> second) {
            super(first, second, LogManager.getLogger(RegionConnections.class));
        }
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

        public static SelectSelfTask create(
                Identifier myId,
                Identifier myRegion,
                Materializer<?> materializer) {
            return new SelectSelfTask(myId, myRegion, 
                    SelectMemberTask.create(materializer));
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

        public static SelectMemberTask<List<ControlSchema.Regions.Entity.Members.Member>> create(Materializer<?> materializer) {
            return new SelectMemberTask<List<ControlSchema.Regions.Entity.Members.Member>>(
                    ControlSchema.Regions.Entity.Members.getMembers(materializer),
                    SelectPresentMemberTask.create(materializer));
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
                    sameThreadExecutor);
        }
    }

    public static class SelectPresentMemberTask implements AsyncFunction<List<ControlSchema.Regions.Entity.Members.Member>, Identifier> {

        public static SelectPresentMemberTask create(
                ClientExecutor<? super Records.Request, ?> client) {
            return new SelectPresentMemberTask(
                    client,
                    SelectRandom.<ControlSchema.Regions.Entity.Members.Member>create());
        }
        
        protected final ClientExecutor<? super Records.Request, ?> client;
        protected final Function<List<ControlSchema.Regions.Entity.Members.Member>, ControlSchema.Regions.Entity.Members.Member> selector;
        
        public SelectPresentMemberTask(
                ClientExecutor<? super Records.Request, ?> client,
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
            return Futures.transform(future, SelectPresentMemberFunction.create(members, selector), sameThreadExecutor);
        }
    }
    
    public static class SelectPresentMemberFunction implements Function<List<Boolean>, Identifier> {

        public static SelectPresentMemberFunction create(
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
