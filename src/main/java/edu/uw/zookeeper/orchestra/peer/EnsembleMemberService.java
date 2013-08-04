package edu.uw.zookeeper.orchestra.peer;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.VolumeDescriptor;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.control.ControlSchema.Ensembles.Entity;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.Records;

@DependsOn({ControlMaterializerService.class})
public class EnsembleMemberService extends DependentService.SimpleDependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(EnsembleConfiguration.module());
        }

        @Provides @Singleton
        public EnsembleMemberService getEnsembleMember(
                EnsembleConfiguration ensembleConfiguration,
                PeerConfiguration conductorConfiguration,
                ControlMaterializerService<?> control,
                ServiceLocator locator,
                DependentServiceMonitor monitor) {
            ControlSchema.Ensembles.Entity myEnsemble = ControlSchema.Ensembles.Entity.of(ensembleConfiguration.getEnsemble());
            ControlSchema.Ensembles.Entity.Peers.Member myMember = ControlSchema.Ensembles.Entity.Peers.Member.of(
                    conductorConfiguration.getView().id(), 
                    ControlSchema.Ensembles.Entity.Peers.of(myEnsemble));
            EnsembleMemberService instance = 
                    monitor.listen(new EnsembleMemberService(myMember, myEnsemble, control, locator));
            return instance;
        }
    }
    
    protected final ControlMaterializerService<?> control;
    protected final ControlSchema.Ensembles.Entity.Peers.Member myMember;
    protected final ControlSchema.Ensembles.Entity myEnsemble;
    protected final RoleOverseer role;
    
    public EnsembleMemberService(
            ControlSchema.Ensembles.Entity.Peers.Member myMember, 
            ControlSchema.Ensembles.Entity myEnsemble,
            ControlMaterializerService<?> control,
            ServiceLocator locator) {
        super(locator);
        this.control = control;
        this.myEnsemble = myEnsemble;
        this.myMember = myMember;
        this.role = new RoleOverseer();
    }
    
    public Identifier id() {
        return myMember.get();
    }
    
    public Identifier ensemble() {
        return myEnsemble.get();
    }
    
    @Override
    protected void startUp() throws Exception {      
        super.startUp();
        
        Materializer<Message.ServerResponse<Records.Response>> materializer = control.materializer();
        Materializer<Message.ServerResponse<Records.Response>>.Operator operator = materializer.operator();

        // Register my identifier
        Message.ServerResponse<Records.Response> result = operator.create(Control.path(myMember.parent())).submit().get();
        Operations.maybeError(result.getRecord(), KeeperException.Code.NODEEXISTS);
        result = operator.create(myMember.path()).submit().get();
        Operations.maybeError(result.getRecord(), KeeperException.Code.NODEEXISTS);

        // Propose myself as leader
        EnsembleRole role = this.role.elect();
        
        // Global barrier - Wait for every ensemble to elect a leader
        Predicate<Materializer<?>> allLeaders = new Predicate<Materializer<?>>() {
            @Override
            public boolean apply(@Nullable Materializer<?> input) {
                ZNodeLabel.Path root = Control.path(ControlSchema.Ensembles.class);
                ZNodeLabel.Component label = ControlSchema.Ensembles.Entity.Leader.LABEL;
                boolean done = true;
                for (Materializer.MaterializedNode e: input.get(root).values()) {
                    if (! e.containsKey(label)) {
                        done = false;
                        break;
                    }
                }
                return done;
            }
        };
        Control.FetchUntil.newInstance(Control.path(ControlSchema.Ensembles.class), allLeaders, materializer).get();
    
        if (EnsembleRole.LEADING == role) {
            // create root volume if there are no volumes
            ZNodeLabel.Path path = Control.path(ControlSchema.Volumes.class);
            operator.getChildren(path).submit().get();
            if (materializer.get(path).isEmpty()) {
                VolumeDescriptor rootVolume = VolumeDescriptor.all();
                ControlSchema.Volumes.Entity.create(rootVolume, materializer, MoreExecutors.sameThreadExecutor()).get();
            }
            
            // Calculate "my" volumes using distance in the identifier space
            Identifier.Space ensembles = Identifier.Space.newInstance();
            for (ZNodeLabel.Component label: materializer.get(Control.path(ControlSchema.Ensembles.class)).keySet()) {
                ensembles.add(Identifier.valueOf(label.toString()));
            }
            List<ControlSchema.Volumes.Entity> myVolumes = Lists.newLinkedList();
            for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> e: materializer.get(Control.path(ControlSchema.Volumes.class)).entrySet()) {
                ControlSchema.Volumes.Entity v = ControlSchema.Volumes.Entity.valueOf(e.getKey().toString());
                if (ensembles.ceiling(v.get()).equals(myEnsemble.get())) {
                    myVolumes.add(v);
                }
            }
            
            // Try to acquire my volumes
            for (ControlSchema.Volumes.Entity v: myVolumes) {
                ControlSchema.Volumes.Entity.Ensemble.create(myEnsemble.get(), v, materializer);
            }
        }        
    }

    protected class RoleOverseer implements FutureCallback<WatchEvent> {
    
        protected final ZNodeLabel.Path leaderPath;
        protected final ControlSchema.Ensembles.Entity.Leader.Proposer<?> proposer;
        protected final Automatons.SynchronizedEventfulAutomaton<EnsembleRole, EnsembleRole> myRole;
        protected final StampedReference.Updater<ControlSchema.Ensembles.Entity.Leader> leader;
        
        public RoleOverseer() {
            this.leaderPath = ZNodeLabel.Path.of(myEnsemble.path(), ControlSchema.Ensembles.Entity.Leader.LABEL);
            this.myRole = Automatons.createSynchronizedEventful(
                    control, Automatons.createSimple(EnsembleRole.LOOKING));
            this.leader = StampedReference.Updater.newInstance(StampedReference.<ControlSchema.Ensembles.Entity.Leader>of(0L, null));
            this.proposer = ControlSchema.Ensembles.Entity.Leader.Proposer.of(
                    control.materializer(), 
                            MoreExecutors.sameThreadExecutor());
            myRole.register(this);
            control.materializer().register(this);
            subscribeLeaderWatch();
        }
        
        public EnsembleRole elect() throws InterruptedException, ExecutionException {
            ControlSchema.Ensembles.Entity.Leader ensembleLeader = proposer.apply(Entity.Leader.of(myMember.get(), myEnsemble)).get();
            return myRoleFor(ensembleLeader);
        }

        @Override
        public void onSuccess(WatchEvent event) {
            if (leaderPath.equals(event.getPath())) {
                switch (event.getType()) {
                case NodeCreated:
                case NodeDataChanged:
                    subscribeLeaderWatch();
                    control.materializer().operator().getData(leaderPath, true).submit();
                    break;
                case NodeDeleted:
                    subscribeLeaderWatch();
                    control.materializer().operator().exists(leaderPath, true).submit();
                    break;
                case NodeChildrenChanged:
                    throw new AssertionError();
                default:
                    break;
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            // TODO
        }

        @Subscribe
        public void handleViewUpdate(ZNodeViewCache.ViewUpdate event) {
            if (leaderPath.equals(event.path())) {
                Materializer.MaterializedNode node = control.materializer().get(leaderPath);
                Identifier value = (node != null) ? (Identifier) node.get().get() : null;
                setLeader(StampedReference.of(event.updated().stamp(), ControlSchema.Ensembles.Entity.Leader.of(value, myEnsemble)));
            }
        }

        @Subscribe
        public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
            if (leaderPath.equals(event.path().get())) {
                if (ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED == event.type()) {
                    setLeader(StampedReference.<ControlSchema.Ensembles.Entity.Leader>of(event.path().stamp(), null));
                }
            }
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> transition) {
            if (transition.type().isAssignableFrom(EnsembleRole.class)) {
                if (EnsembleRole.LOOKING == transition.to()) {
                    try {
                        elect();
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
            }
        }
        
        protected EnsembleRole myRoleFor(ControlSchema.Ensembles.Entity.Leader leader) {
            if (leader == null) {
                return EnsembleRole.LOOKING;
            } else if (myMember.get().equals(leader.get())) {
                return EnsembleRole.LEADING;
            } else {
                return EnsembleRole.FOLLOWING;
            }
        }

        protected void setLeader(StampedReference<ControlSchema.Ensembles.Entity.Leader> newLeader) {
            StampedReference<ControlSchema.Ensembles.Entity.Leader> prevLeader = leader.setIfGreater(newLeader);
            if (prevLeader.stamp() < newLeader.stamp()) {
                myRole.apply(myRoleFor(newLeader.get()));
            }
        }

        protected void subscribeLeaderWatch() {
            Futures.addCallback(control.watches().subscribe(leaderPath, EnumSet.allOf(Watcher.Event.EventType.class)), this);
        }
    }
}
