package edu.uw.zookeeper.orchestra.frontend;

import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.common.CachedFunction;
import edu.uw.zookeeper.orchestra.common.CachedLookup;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.SharedLookup;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.peer.EnsembleConfiguration;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.protocol.proto.Records;

@DependsOn({PeerToEnsembleLookup.class, PeerConnectionsService.class})
public class EnsembleConnectionsService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}

        @Provides @Singleton
        public EnsembleConnectionsService getEnsembleConnectionsService(
                PeerConfiguration peer,
                EnsembleConfiguration ensemble,
                ControlMaterializerService<?> control,
                PeerToEnsembleLookup peerToEnsemble,
                ClientPeerConnections peerConnections) {
            return EnsembleConnectionsService.newInstance(
                            peer.getView().id(),
                            ensemble.getEnsemble(),
                            peerToEnsemble.get().asLookup().first(),
                            peerConnections, 
                            control);
        }

        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { PeerToEnsembleLookup.module() };
            return modules;
        }
    }

    public static EnsembleConnectionsService newInstance(
            Identifier myId,
            Identifier myEnsemble,
            Function<? super Identifier, Identifier> peerToEnsemble,
            ClientPeerConnections peerConnections,
            ControlMaterializerService<?> control) {
        CachedLookup<Identifier, Identifier> selectedPeers = 
                CachedLookup.create(
                        SelectSelfTask.create(
                                myId, myEnsemble, control.materializer()));
        return new EnsembleConnectionsService(
                selectedPeers,
                peerToEnsemble,
                peerConnections,
                control);
    }
    
    protected final ControlMaterializerService<?> control;
    protected final ClientPeerConnections peerConnections;
    protected final CachedLookup<Identifier, Identifier> selectedPeers;
    protected final EnsembleConnections ensembleConnections;
    protected final Function<? super Identifier, Identifier> peerToEnsemble;
    
    protected EnsembleConnectionsService(
            CachedLookup<Identifier, Identifier> selectedPeers,
            Function<? super Identifier, Identifier> peerToEnsemble,
            ClientPeerConnections peerConnections,
            ControlMaterializerService<?> control) {
        this.control = control;
        this.peerToEnsemble = peerToEnsemble;
        this.peerConnections = peerConnections;
        this.selectedPeers = selectedPeers;
        this.ensembleConnections = EnsembleConnections.create(
                selectedPeers.asLookup(), peerConnections.asLookup());
    }

    public CachedLookup<Identifier, Identifier> getPeerForEnsemble() {
        return selectedPeers;
    }

    public CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> getConnectionForEnsemble() {
        return ensembleConnections;
    }

    @Subscribe
    public void handleClientPeerConnection(ClientPeerConnection<?> connection) {
        new UnselectOnClose(connection);
    }

    @Override
    protected void startUp() throws Exception {
        peerConnections.register(this);
        for (ClientPeerConnection<?> c: peerConnections) {
            handleClientPeerConnection(c);
        }

        // establish a peer connection per ensemble
        Futures.transform(
                ControlSchema.Ensembles.getEnsembles(control.materializer()), 
                new AsyncFunction<List<ControlSchema.Ensembles.Entity>, List<ClientPeerConnection<Connection<? super MessagePacket>>>>() {
                    @Override
                    public ListenableFuture<List<ClientPeerConnection<Connection<? super MessagePacket>>>> apply(List<ControlSchema.Ensembles.Entity> input)
                            throws Exception {
                        List<ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>>> futures = Lists.newArrayListWithCapacity(input.size());
                        for (ControlSchema.Ensembles.Entity e: input) {
                            futures.add(Futures.transform(
                                    selectedPeers.asLookup().apply(e.get()), 
                                    ensembleConnections));
                        }
                        return Futures.successfulAsList(futures);
                    }
                }).get();
    }
    
    @Override
    protected void shutDown() throws Exception {
    }
    
    protected class UnselectOnClose {
        protected final ClientPeerConnection<?> connection;
        
        public UnselectOnClose(ClientPeerConnection<?> connection) {
            this.connection = connection;
            connection.register(this);
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if ((event.to() == Connection.State.CONNECTION_CLOSING) ||
                    (event.to() == Connection.State.CONNECTION_CLOSED)) {
                try {
                    connection.unregister(this);
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

    public static class EnsembleConnections extends CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> {
    
        public static EnsembleConnections create(
                final CachedFunction<Identifier, Identifier> peers,
                final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectFunction) {
            return new EnsembleConnections(
                    new Function<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>>() {
                        @Override
                        public @Nullable
                        ClientPeerConnection<Connection<? super MessagePacket>> apply(Identifier ensemble) {
                            Identifier peer = peers.first().apply(ensemble);
                            if (peer != null) {
                                return connectFunction.first().apply(peer);
                            } else {
                                return null;
                            }
                        }
                    }, 
                    SharedLookup.<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>>create(
                            new AsyncFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>>() {
                                @Override
                                public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> apply(
                                        Identifier ensemble) throws Exception {
                                    return Futures.transform(
                                            peers.apply(ensemble),
                                            connectFunction);
                                }
                            }));
        }
        
        protected EnsembleConnections(
                Function<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> first,
                AsyncFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> second) {
            super(first, second);
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
                Identifier myEnsemble,
                Materializer<?> materializer) {
            return new SelectSelfTask(myId, myEnsemble, SelectMemberTask.create(materializer));
        }
        
        protected final Identifier myId;
        protected final Identifier myEnsemble;
        protected final AsyncFunction<Identifier, Identifier> fallback;
        
        public SelectSelfTask(
                Identifier myId,
                Identifier myEnsemble,
                AsyncFunction<Identifier, Identifier> fallback) {
            this.myId = myId;
            this.myEnsemble = myEnsemble;
            this.fallback = fallback;
        }

        @Override
        public ListenableFuture<Identifier> apply(Identifier ensemble)
                throws Exception {
            return (ensemble == myEnsemble) ?
                    Futures.immediateFuture(myId) : 
                        fallback.apply(ensemble);
        }
    }
    
    public static class SelectMemberTask<T> implements AsyncFunction<Identifier, Identifier> {

        public static SelectMemberTask<List<ControlSchema.Ensembles.Entity.Peers.Member>> create(Materializer<?> materializer) {
            return new SelectMemberTask<List<ControlSchema.Ensembles.Entity.Peers.Member>>(
                    ControlSchema.Ensembles.Entity.Peers.getMembers(materializer),
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
            return Futures.transform(memberLookup.apply(ensemble), selector);
        }
    }

    public static class SelectPresentMemberTask implements AsyncFunction<List<ControlSchema.Ensembles.Entity.Peers.Member>, Identifier> {

        public static SelectPresentMemberTask create(
                ClientExecutor<? super Records.Request, ?> client) {
            return new SelectPresentMemberTask(
                    client,
                    SelectRandom.<ControlSchema.Ensembles.Entity.Peers.Member>create());
        }
        
        protected final ClientExecutor<? super Records.Request, ?> client;
        protected final Function<List<ControlSchema.Ensembles.Entity.Peers.Member>, ControlSchema.Ensembles.Entity.Peers.Member> selector;
        
        public SelectPresentMemberTask(
                ClientExecutor<? super Records.Request, ?> client,
                Function<List<ControlSchema.Ensembles.Entity.Peers.Member>, ControlSchema.Ensembles.Entity.Peers.Member> selector) {
            this.client = client;
            this.selector = selector;
        }
        
        @Override
        public ListenableFuture<Identifier> apply(
                List<ControlSchema.Ensembles.Entity.Peers.Member> members) {
            List<ListenableFuture<Boolean>> presence = Lists.newArrayListWithCapacity(members.size());
            for (ControlSchema.Ensembles.Entity.Peers.Member e: members) {
                presence.add(ControlSchema.Peers.Entity.of(e.get()).presence().exists(client));
            }
            ListenableFuture<List<Boolean>> future = Futures.successfulAsList(presence);
            return Futures.transform(future, SelectPresentMemberFunction.create(members, selector));
        }
    }
    
    public static class SelectPresentMemberFunction implements Function<List<Boolean>, Identifier> {

        public static SelectPresentMemberFunction create(
                List<ControlSchema.Ensembles.Entity.Peers.Member> members,
                Function<List<ControlSchema.Ensembles.Entity.Peers.Member>, ControlSchema.Ensembles.Entity.Peers.Member> selector) {
            return new SelectPresentMemberFunction(members, selector);
        }
        
        protected final List<ControlSchema.Ensembles.Entity.Peers.Member> members;
        protected final Function<List<ControlSchema.Ensembles.Entity.Peers.Member>, ControlSchema.Ensembles.Entity.Peers.Member> selector;
        
        public SelectPresentMemberFunction(
                List<ControlSchema.Ensembles.Entity.Peers.Member> members,
                Function<List<ControlSchema.Ensembles.Entity.Peers.Member>, ControlSchema.Ensembles.Entity.Peers.Member> selector) {
            this.members = members;
            this.selector = selector;
        }
        
        @Override
        public Identifier apply(List<Boolean> presence) {
            List<ControlSchema.Ensembles.Entity.Peers.Member> living = Lists.newArrayListWithCapacity(members.size());
            for (int i=0; i<members.size(); ++i) {
                try {
                    if (Boolean.TRUE.equals(presence.get(i))) {
                        living.add(members.get(i));
                    }
                } catch (Exception e) {}
            }
            ControlSchema.Ensembles.Entity.Peers.Member selected = selector.apply(living);
            return (selected == null) ? null : selected.get();
        }
    }
}
