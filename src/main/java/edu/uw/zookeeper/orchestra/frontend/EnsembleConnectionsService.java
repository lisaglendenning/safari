package edu.uw.zookeeper.orchestra.frontend;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.peer.EnsembleConfiguration;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.protocol.proto.Records;

@DependsOn({PeerConnectionsService.class})
public class EnsembleConnectionsService extends DependentService.SimpleDependentService implements Iterable<Identifier> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(PeerConnectionsService.module());
        }

        @Provides @Singleton
        public EnsembleConnectionsService getEnsemblePeerService(
                PeerConfiguration peer,
                EnsembleConfiguration ensemble,
                ControlMaterializerService<?> controlClient,
                PeerConnectionsService<?> peerConnections,
                ServiceLocator locator,
                DependentServiceMonitor monitor) throws InterruptedException, ExecutionException, KeeperException {
            EnsembleConnectionsService instance = 
                    monitor.listen(
                            new EnsembleConnectionsService(
                                peer.getView().id(),
                                ensemble.getEnsemble(),
                                peerConnections, 
                                controlClient, 
                                locator));
            return instance;
        }
    }

    protected final Identifier myId;
    protected final Identifier myEnsemble;
    protected final ControlMaterializerService<?> controlClient;
    protected final PeerConnectionsService<?> peerConnections;
    protected final ConcurrentMap<Identifier, Identifier> selectedPeers;
    protected final CachedFunction<Identifier, List<Orchestra.Ensembles.Entity.Peers.Member>> memberLookup;
    protected final CachedFunction<Identifier, Identifier> selectPeers;
    protected final AsyncFunction<List<Orchestra.Ensembles.Entity.Peers.Member>, Identifier> selectMemberFunction;
    protected final CachedFunction<Identifier, ClientPeerConnection> connectFunction;
    protected final CachedFunction<Identifier, ClientPeerConnection> ensembleConnections;
    
    public EnsembleConnectionsService(
            Identifier myId,
            Identifier myEnsemble,
            PeerConnectionsService<?> peerConnections,
            ControlMaterializerService<?> controlClient,
            ServiceLocator locator) {
        super(locator);
        this.myId = myId;
        this.myEnsemble = myEnsemble;
        this.controlClient = controlClient;
        this.peerConnections = peerConnections;
        this.selectedPeers = new MapMaker().makeMap();
        selectedPeers.put(myEnsemble, myId);
        this.memberLookup = Orchestra.Ensembles.Entity.Peers.getMembers(controlClient.materializer());
        this.selectMemberFunction = SelectMemberTask.of(controlClient.materializer());
        this.selectPeers = CachedFunction.create(
                new Function<Identifier, Identifier>() {
                    @Override
                    public @Nullable
                    Identifier apply(Identifier ensemble) {
                        return selectedPeers.get(ensemble);
                    }
                }, 
                new AsyncFunction<Identifier, Identifier>() {
                    @Override
                    public ListenableFuture<Identifier> apply(
                            final Identifier ensemble) throws Exception {
                        Identifier peer = selectedPeers.get(ensemble);
                        if (peer != null) {
                            return Futures.immediateFuture(peer);
                        } else {
                            return Futures.transform(
                                    Futures.transform(
                                    memberLookup.apply(ensemble),
                                    selectMemberFunction),
                                    new Function<Identifier, Identifier>() {
                                        @Override
                                        public @Nullable
                                        Identifier apply(
                                                @Nullable Identifier peer) {
                                            selectedPeers.putIfAbsent(ensemble, peer);
                                            return selectedPeers.get(ensemble);
                                        }
                                    });
                        }
                    }
                });
        this.connectFunction = peerConnections.clients().connectFunction();
        this.ensembleConnections = CachedFunction.create(
                new Function<Identifier, ClientPeerConnection>() {
                    @Override
                    public @Nullable
                    ClientPeerConnection apply(Identifier ensemble) {
                        Identifier peer = selectedPeers.get(ensemble);
                        if (peer != null) {
                            return connectFunction.first().apply(peer);
                        } else {
                            return null;
                        }
                    }
                }, 
                new AsyncFunction<Identifier, ClientPeerConnection>() {
                    @Override
                    public ListenableFuture<ClientPeerConnection> apply(
                            Identifier ensemble) throws Exception {
                        return Futures.transform(
                                selectPeers.apply(ensemble),
                                connectFunction);
                    }
                });
    }
    
    public CachedFunction<Identifier, Identifier> getPeerForEnsemble() {
        return selectPeers;
    }

    public CachedFunction<Identifier, ClientPeerConnection> getConnectionForEnsemble() {
        return ensembleConnections;
    }

    @Override
    public Iterator<Identifier> iterator() {
        return selectedPeers.keySet().iterator();
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        Futures.transform(
                Orchestra.Ensembles.getEnsembles(controlClient.materializer()), 
                new AsyncFunction<List<Orchestra.Ensembles.Entity>, List<ClientPeerConnection>>() {
                    @Override
                    public ListenableFuture<List<ClientPeerConnection>> apply(List<Orchestra.Ensembles.Entity> input)
                            throws Exception {
                        List<ListenableFuture<ClientPeerConnection>> futures = Lists.newArrayListWithCapacity(input.size());
                        for (Orchestra.Ensembles.Entity e: input) {
                            futures.add(Futures.transform(
                                    selectPeers.second().apply(e.get()), 
                                    ensembleConnections));
                        }
                        return Futures.successfulAsList(futures);
                    }
                }).get();
    }

    public static class SelectRandom<V> implements Function<List<V>, V> {
        
        public static <V> SelectRandom<V> of() {
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

    public static class SelectMemberTask implements AsyncFunction<List<Orchestra.Ensembles.Entity.Peers.Member>, Identifier> {

        public static SelectMemberTask of(
                ClientExecutor<? super Records.Request, ?, ?> client) {
            return new SelectMemberTask(
                    client,
                    new SelectRandom<Orchestra.Ensembles.Entity.Peers.Member>());
        }
        
        protected final ClientExecutor<? super Records.Request, ?, ?> client;
        protected final Function<List<Orchestra.Ensembles.Entity.Peers.Member>, Orchestra.Ensembles.Entity.Peers.Member> selector;
        
        public SelectMemberTask(
                ClientExecutor<? super Records.Request, ?, ?> client,
                Function<List<Orchestra.Ensembles.Entity.Peers.Member>, Orchestra.Ensembles.Entity.Peers.Member> selector) {
            this.client = client;
            this.selector = selector;
        }
        
        @Override
        public ListenableFuture<Identifier> apply(
                List<Orchestra.Ensembles.Entity.Peers.Member> members) {
            List<ListenableFuture<Boolean>> presence = Lists.newArrayListWithCapacity(members.size());
            for (Orchestra.Ensembles.Entity.Peers.Member e: members) {
                presence.add(Orchestra.Peers.Entity.of(e.get()).presence().exists(client));
            }
            ListenableFuture<List<Boolean>> future = Futures.successfulAsList(presence);
            return Futures.transform(future, SelectPresentMemberFunction.of(members, selector));
        }
    }
    
    public static class SelectPresentMemberFunction implements Function<List<Boolean>, Identifier> {

        public static SelectPresentMemberFunction of(
                List<Orchestra.Ensembles.Entity.Peers.Member> members,
                Function<List<Orchestra.Ensembles.Entity.Peers.Member>, Orchestra.Ensembles.Entity.Peers.Member> selector) {
            return new SelectPresentMemberFunction(members, selector);
        }
        
        protected final List<Orchestra.Ensembles.Entity.Peers.Member> members;
        protected final Function<List<Orchestra.Ensembles.Entity.Peers.Member>, Orchestra.Ensembles.Entity.Peers.Member> selector;
        
        public SelectPresentMemberFunction(
                List<Orchestra.Ensembles.Entity.Peers.Member> members,
                Function<List<Orchestra.Ensembles.Entity.Peers.Member>, Orchestra.Ensembles.Entity.Peers.Member> selector) {
            this.members = members;
            this.selector = selector;
        }
        
        @Override
        public Identifier apply(List<Boolean> presence) {
            List<Orchestra.Ensembles.Entity.Peers.Member> living = Lists.newArrayListWithCapacity(members.size());
            for (int i=0; i<members.size(); ++i) {
                if (Boolean.TRUE.equals(presence.get(i))) {
                    living.add(members.get(i));
                }
            }
            Orchestra.Ensembles.Entity.Peers.Member selected = selector.apply(living);
            return (selected == null) ? null : selected.get();
        }
    }
}
