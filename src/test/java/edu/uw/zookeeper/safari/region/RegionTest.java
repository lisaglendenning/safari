package edu.uw.zookeeper.safari.region;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ForwardingBlockingQueue;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.storage.StorageModules;

@RunWith(JUnit4.class)
public class RegionTest extends AbstractMainTest {
    
    public static class TransitionListenerQueue<V> extends ForwardingBlockingQueue<Automaton.Transition<V>> implements Automatons.AutomatonListener<V> {
        
        public static <V> TransitionListenerQueue<V> create(BlockingQueue<Automaton.Transition<V>> delegate) {
            return new TransitionListenerQueue<V>(delegate);
        }
        
        private final BlockingQueue<Automaton.Transition<V>> delegate;
        
        public TransitionListenerQueue(BlockingQueue<Automaton.Transition<V>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handleAutomatonTransition(Automaton.Transition<V> transition) {
            add(transition);
        }

        @Override
        protected BlockingQueue<Automaton.Transition<V>> delegate() {
            return delegate;
        }
    }

    @Test(timeout=15000)
    public void testStartAndStop() throws Exception {
        final long pause = 5000L;
        final Component<Named> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final Component<?> server = StorageModules.newStorageSingletonEnsemble(root);
        final Component<?> client = RegionModules.newSingletonRegionMember(
                server, ImmutableList.of(root, control));
        pauseWithComponents(
                ImmutableList.<Component<?>>of(root, control, server, client), 
                pause);
    }
    
    @Test(timeout=30000)
    public void testRegionStartAndStop() throws Exception {
        final int size = 3;
        final long pause = 10000L;
        final Component<Named> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final List<Component<?>> storage = StorageModules.newStorageEnsemble(root, size);
        final List<Component<?>> members = RegionModules.newRegion(storage, ImmutableList.of(root, control));
        pauseWithComponents(
                ImmutableList.<Component<?>>builder().add(root).add(control).addAll(storage).addAll(members).build(), 
                pause);
    }

    @Test(timeout=60000)
    public void testLeaderStop() throws Exception {
        final int size = 3;
        final Component<Named> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final List<Component<?>> storage = StorageModules.newStorageEnsemble(root, size);
        final List<Component<?>> members = RegionModules.newRegion(storage, ImmutableList.of(root, control));
        final Injector world = monitored(
                ImmutableList.<Component<?>>builder().add(root).add(control).addAll(storage).addAll(members).build(),
                Modules.NonStoppingServiceMonitorProvider.class);
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                List<Pair<Automatons.EventfulAutomaton<RegionRole, LeaderEpoch>, TransitionListenerQueue<RegionRole>>> roles = Lists.transform(members, new Function<Component<?>, Pair<Automatons.EventfulAutomaton<RegionRole, LeaderEpoch>, TransitionListenerQueue<RegionRole>>>() {
                    @Override
                    public Pair<Automatons.EventfulAutomaton<RegionRole, LeaderEpoch>, TransitionListenerQueue<RegionRole>> apply(
                            Component<?> component) {
                        Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> role = component.injector().getInstance(Key.get(new TypeLiteral<Automatons.EventfulAutomaton<RegionRole, LeaderEpoch>>(){}));
                        TransitionListenerQueue<RegionRole> transitions = TransitionListenerQueue.create(Queues.<Automaton.Transition<RegionRole>>newLinkedBlockingQueue());
                        role.subscribe(transitions);
                        return Pair.create(role, transitions);
                    }
                });

                LeaderEpoch epoch = null;
                Component<?> leader = null;
                for (int i=0; i<size; ++i) {
                    Component<?> member = members.get(i);
                    Pair<Automatons.EventfulAutomaton<RegionRole, LeaderEpoch>, TransitionListenerQueue<RegionRole>> role = roles.get(i);
                    RegionRole r = role.first().state();
                    switch (r.getRole()) {
                    case LEADING:
                    case FOLLOWING:
                        role.second().poll();
                        break;
                    default:
                        try {
                            r = role.second().take().to();
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        }
                        break;
                    }
                    Identifier region = member.injector().getInstance(Key.get(Identifier.class, Module.annotation()));
                    LockableZNodeCache<ControlZNode<?>,?,?> cache = member.injector().getInstance(ControlClientService.class).materializer().cache();
                    cache.lock().readLock().lock();
                    try {
                        if (epoch == null) {
                            epoch = ControlSchema.Safari.Regions.Region.Leader.get(cache.cache(), region).getLeaderEpoch().get();
                        } else {
                            assertEquals(epoch, ControlSchema.Safari.Regions.Region.Leader.get(cache.cache(), region).getLeaderEpoch().get());
                        }
                    } finally {
                        cache.lock().readLock().unlock();
                    }
                    assertEquals(epoch, r.getLeader());
                    if (member.injector().getInstance(Key.get(Identifier.class, Peer.class)).equals(epoch.getLeader())) {
                        assertNull(leader);
                        leader = member;
                        assertEquals(r.getRole(), EnsembleRole.LEADING);
                    } else {
                        assertEquals(r.getRole(), EnsembleRole.FOLLOWING);
                    }
                }
                
                logger.info("Stopping leader {}", epoch);
                Services.stopAndWait(leader.injector().getInstance(ServiceMonitor.class));
                
                LeaderEpoch newEpoch = null;
                Component<?> newLeader = null;
                for (int i=0; i<size; ++i) {
                    Component<?> member = members.get(i);
                    if (member.equals(leader)) {
                        continue;
                    }
                    Pair<Automatons.EventfulAutomaton<RegionRole, LeaderEpoch>, TransitionListenerQueue<RegionRole>> role = roles.get(i);
                    RegionRole r = role.first().state();
                    switch (r.getRole()) {
                    case LEADING:
                        role.second().poll();
                        break;
                    default:
                        try {
                            r = role.second().take().to();
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        }
                        break;
                    }
                    
                    Identifier region = member.injector().getInstance(Key.get(Identifier.class, Module.annotation()));
                    LockableZNodeCache<ControlZNode<?>,?,?> cache = member.injector().getInstance(ControlClientService.class).materializer().cache();
                    cache.lock().readLock().lock();
                    try {
                        if (newEpoch == null) {
                            newEpoch = ControlSchema.Safari.Regions.Region.Leader.get(cache.cache(), region).getLeaderEpoch().get();
                            assertNotEquals(epoch, newEpoch);
                            assertTrue(newEpoch.getEpoch() > epoch.getEpoch());
                            assertNotEquals(newEpoch.getLeader(), epoch.getLeader());
                        } else {
                            assertEquals(newEpoch, ControlSchema.Safari.Regions.Region.Leader.get(cache.cache(), region).getLeaderEpoch().get());
                        }
                    } finally {
                        cache.lock().readLock().unlock();
                    }
                    assertEquals(newEpoch, r.getLeader());
                    if (member.injector().getInstance(Key.get(Identifier.class, Peer.class)).equals(newEpoch.getLeader())) {
                        assertNull(newLeader);
                        newLeader = member;
                        assertEquals(r.getRole(), EnsembleRole.LEADING);
                    } else {
                        assertEquals(r.getRole(), EnsembleRole.FOLLOWING);
                    }
                }
                assertNotNull(newLeader);
                assertNotEquals(leader, newLeader);

                return null;
            }
        };
        
        callWithService(world, callable);
    }
}
