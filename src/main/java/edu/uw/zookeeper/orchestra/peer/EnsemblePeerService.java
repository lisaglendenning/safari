package edu.uw.zookeeper.orchestra.peer;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.zookeeper.KeeperException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.SettableFuturePromise;

@DependsOn({PeerConnectionsService.class})
public class EnsemblePeerService extends DependentService.SimpleDependentService implements Iterable<Identifier> {

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
        public EnsemblePeerService getEnsemblePeerService(
                ControlMaterializerService<?> controlClient,
                PeerConnectionsService<?> peerConnections,
                ServiceLocator locator,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            EnsemblePeerService instance = new EnsemblePeerService(peerConnections, controlClient, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    protected final ControlMaterializerService<?> controlClient;
    protected final PeerConnectionsService<?> peerConnections;
    protected final ConcurrentMap<Identifier, Identifier> ensemblePeers;
    
    public EnsemblePeerService(
            PeerConnectionsService<?> peerConnections,
            ControlMaterializerService<?> controlClient,
            ServiceLocator locator) {
        super(locator);
        this.controlClient = controlClient;
        this.peerConnections = peerConnections;
        this.ensemblePeers = new ConcurrentHashMap<Identifier, Identifier>();
    }
    
    public class LookupMembersTask implements Callable<List<Orchestra.Ensembles.Entity.Peers.Member>> {

        protected final Identifier ensemble;
        
        public LookupMembersTask(Identifier ensemble) {
            this.ensemble = ensemble;
        }

        @Override
        public List<Orchestra.Ensembles.Entity.Peers.Member> call() throws InterruptedException, ExecutionException, KeeperException {
            Orchestra.Ensembles.Entity.Peers conductors = Orchestra.Ensembles.Entity.Peers.of(Orchestra.Ensembles.Entity.of(ensemble));
            return conductors.lookup(controlClient.materializer());
        }
    }
    
    public class SelectPeerTask implements Callable<Identifier> {

        protected final Identifier ensemble;
        
        public SelectPeerTask(Identifier ensemble) {
            this.ensemble = ensemble;
        }
        
        @Override
        public Identifier call() throws InterruptedException, ExecutionException, KeeperException {
            List<Orchestra.Ensembles.Entity.Peers.Member> members = new LookupMembersTask(ensemble).call();
            Identifier peer = null;
            Collections.shuffle(members);
            for (Orchestra.Ensembles.Entity.Peers.Member e: members) {
                if (peerConnections.new PresenceTask(e.get()).call()) {
                    peer = e.get();
                    break;
                }
            }
            return peer;
        }
    }

    public Identifier getPeerForEnsemble(Identifier ensemble) throws InterruptedException, ExecutionException, KeeperException {
        Identifier peer = ensemblePeers.get(ensemble);
        if (peer == null) {
            Identifier myEnsemble = locator().getInstance(EnsembleConfiguration.class).getEnsemble();
            if (ensemble.equals(myEnsemble)) {
                peer = locator().getInstance(PeerConfiguration.class).getView().id();
            } else {
                peer = new SelectPeerTask(ensemble).call();
            }
            ensemblePeers.putIfAbsent(ensemble, peer);
            peer = ensemblePeers.get(ensemble);
        }
        return peer;
    }

    public PeerConnectionsService<?>.ClientPeerConnection getConnectionForEnsemble(Identifier ensemble) throws InterruptedException, ExecutionException, KeeperException {
        PeerConnectionsService<?>.ClientPeerConnection connection = null;
        Identifier peer = getPeerForEnsemble(ensemble);
        if (peer != null) {
            // TODO: send a list of existing sessions to new connections somewhere
            connection = peerConnections.clients().connect(peer, MoreExecutors.sameThreadExecutor()).get();
        }
        return connection;
    }

    @Override
    public Iterator<Identifier> iterator() {
        return ensemblePeers.keySet().iterator();
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        ConnectToAll connectToAll = new ConnectToAll();
        connectToAll.call().get();
    }

    protected class ConnectToAll extends TreeFetcher<Message.ClientRequest<?>, Message.ServerResponse<?>> {
        
        protected ConnectToAll() {
            super(TreeFetcher.Parameters.of(EnumSet.of(OpCode.GET_CHILDREN), false), 
                    Control.path(Orchestra.Ensembles.class),
                    controlClient.materializer(), 
                    SettableFuturePromise.<ZNodeLabel.Path>create(),
                    MoreExecutors.sameThreadExecutor());
        }
        
        @Override
        protected ConnectToAllActor newActor() {
            return new ConnectToAllActor(promise, parameters, root, client, executor);
        }

        protected class ConnectToAllActor extends TreeFetcherActor<Message.ClientRequest<?>, Message.ServerResponse<?>> {
        
            protected final FutureQueue<ListenableFuture<PeerConnectionsService<?>.ClientPeerConnection>> pendingConnects;
            
            protected ConnectToAllActor(
                    Promise<ZNodeLabel.Path> promise,
                    Parameters parameters, 
                    ZNodeLabel.Path root, 
                    ClientExecutor<Operation.Request, Message.ClientRequest<?>, Message.ServerResponse<?>> client,
                    Executor executor) {
                super(promise, parameters, root, client, executor);
                this.pendingConnects = FutureQueue.create();
            }

            @Override
            protected void doRun() throws Exception {
                ListenableFuture<PeerConnectionsService<?>.ClientPeerConnection> next;
                while ((next = pendingConnects.poll()) != null) {
                    try {
                        next.get();
                    } catch (Exception e) {
                        // TODO
                        stop();
                    }
                }
                
                super.doRun();
            }
            
            @Override
            protected void applyPendingResult(Pair<Message.ClientRequest<?>, Message.ServerResponse<?>> result) throws KeeperException, InterruptedException, ExecutionException {
                Records.Request request = result.first().getRecord();
                Records.Response reply = Operations.maybeError(result.second().getRecord(), KeeperException.Code.NONODE, result.toString());
                if (reply instanceof Records.ChildrenGetter) {
                    ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
                    ZNodeLabel.Path pathHead = (ZNodeLabel.Path) path.head();
                    ZNodeLabel.Component pathTail = path.tail();
                    if (Orchestra.Ensembles.Entity.Peers.LABEL.equals(pathTail)) {
                        Identifier ensemble = Identifier.valueOf(controlClient.materializer().get(pathHead).parent().orNull().label().toString());
                        ListenableFutureTask<PeerConnectionsService<?>.ClientPeerConnection> task = ListenableFutureTask.create(new ConnectTask(ensemble));
                        pendingConnects.add(task);
                        task.addListener(this, executor);
                        executor.execute(task);
                    }
                    if (root.equals(path) 
                            || root.equals(pathHead) 
                            || Orchestra.Ensembles.Entity.Peers.LABEL.equals(pathTail)) {
                        for (String child: ((Records.ChildrenGetter) reply).getChildren()) {
                            send(ZNodeLabel.Path.of(path, ZNodeLabel.Component.of(child)));
                        }
                    }
                }
            }
            
            @Override
            protected void runExit() {
                if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                    if (!mailbox.isEmpty() || !pending.isEmpty() || !pendingConnects.isEmpty()) {
                        schedule();
                    } else if (pending.delegate().isEmpty() && pendingConnects.delegate().isEmpty()) {
                        // We're done!
                        stop();
                    }
                }
            }
        }

        protected class ConnectTask implements Callable<PeerConnectionsService<?>.ClientPeerConnection> {
            
            protected final Identifier ensemble;
            
            protected ConnectTask(Identifier ensemble) {
                this.ensemble = ensemble;
            }

            @Override
            public PeerConnectionsService<?>.ClientPeerConnection call() throws Exception {
                return getConnectionForEnsemble(ensemble);
            }
        }
    }
}
