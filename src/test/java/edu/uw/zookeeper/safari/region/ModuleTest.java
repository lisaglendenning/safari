package edu.uw.zookeeper.safari.region;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.storage.StorageModules;

@RunWith(JUnit4.class)
public class ModuleTest extends AbstractMainTest {

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
        final TimeValue timeOut = TimeValue.seconds(10L);
        final Component<Named> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final List<Component<?>> storage = StorageModules.newStorageEnsemble(root, size);
        final List<Component<?>> members = RegionModules.newRegion(storage, ImmutableList.of(root, control));
        final Injector world = nonstopping(
                ImmutableList.<Component<?>>builder().add(root).add(control).addAll(storage).addAll(members).build());
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LeaderEpoch epoch = null;
                Component<?> leader = null;
                List<ListenableFuture<RegionRoleService>> nextRoles = Lists.newLinkedList();
                for (int i=0; i<size; ++i) {
                    Component<?> member = members.get(i);
                    FutureTransition<RegionRoleService> role = member.injector().getInstance(Key.get(new TypeLiteral<FutureTransition<RegionRoleService>>(){}));
                    RegionRole current;
                    do {
                        current = null;
                        if (role.getCurrent().isPresent()) {
                            current = role.getCurrent().get().getRole();
                            if (current.getRole() == EnsembleRole.UNKNOWN) {
                                current = null;
                            }
                        }
                        if (current == null) {
                            current = role.getNext().get(timeOut.value(), timeOut.unit()).getRole();
                        }
                    } while (current.getRole() == EnsembleRole.UNKNOWN);
                    if (epoch == null) {
                        epoch = current.getEpoch();
                    } else {
                        assertEquals(epoch, current.getEpoch());
                    }
                    if (current.getRole() == EnsembleRole.LEADING) {
                        assertNull(leader);
                        leader = member;
                    }
                    nextRoles.add(member.injector().getInstance(Key.get(new TypeLiteral<FutureTransition<RegionRoleService>>(){})).getNext());
                }
                assertNotNull(leader);
                assertEquals(epoch.getLeader(), leader.injector().getInstance(Key.get(Identifier.class, Peer.class)));
                
                logger.info("Stopping leader {}", epoch);
                Services.stopAndWait(leader.injector().getInstance(ServiceMonitor.class));
                
                LeaderEpoch newEpoch = null;
                Component<?> newLeader = null;
                for (int i=0; i<size; ++i) {
                    Component<?> member = members.get(i);
                    if (member == leader) {
                        continue;
                    }
                    RegionRole current = nextRoles.get(i).get().getRole();
                    while (current.getRole() == EnsembleRole.UNKNOWN) {
                        current = null;
                        FutureTransition<RegionRoleService> role = member.injector().getInstance(Key.get(new TypeLiteral<FutureTransition<RegionRoleService>>(){}));
                        if (role.getCurrent().isPresent()) {
                            current = role.getCurrent().get().getRole();
                            if (current.getRole() == EnsembleRole.UNKNOWN) {
                                current = null;
                            }
                        }
                        if (current == null) {
                            current = role.getNext().get(timeOut.value(), timeOut.unit()).getRole();
                        }
                    }
                    if (newEpoch == null) {
                        newEpoch = current.getEpoch();
                    } else {
                        assertEquals(newEpoch, current.getEpoch());
                    }
                    if (current.getRole() == EnsembleRole.LEADING) {
                        assertNull(newLeader);
                        newLeader = member;
                    }
                }
                assertNotNull(newLeader);
                assertNotEquals(leader, newLeader);
                assertNotEquals(epoch, newEpoch);
                assertTrue(newEpoch.getEpoch() > epoch.getEpoch());
                assertEquals(newEpoch.getLeader(), newLeader.injector().getInstance(Key.get(Identifier.class, Peer.class)));

                return null;
            }
        };
        
        callWithService(world, callable);
    }
}
