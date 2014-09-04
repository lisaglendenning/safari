package edu.uw.zookeeper.safari.region;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.peer.Peer;

public class RegionRoleService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}

        @Provides @Singleton
        public Automatons.EventfulAutomaton<RegionRole, LeaderEpoch> getRegionRole(
                @Peer Identifier peer) {
            return Automatons.createSynchronizedEventful(
                    Automatons.createEventful(RegionRoleAutomaton.unknown(peer)));
        }
        
        @Provides @Singleton
        public ListenersFactory newListenersFactory() {
            return ListenersFactory.create();
        }

        @Provides @Singleton
        public RoleListener newRoleListener(
                ListenersFactory factory,
                Automatons.EventfulAutomaton<RegionRole,LeaderEpoch> role) {
            return RoleListener.create(factory, role);
        }

        @Provides
        public FutureTransition<RegionRoleService> getRole(
                Supplier<FutureTransition<RegionRoleService>> role) {
            return role.get();
        }
        
        @Override
        protected void configure() {
            bind(new TypeLiteral<Supplier<FutureTransition<RegionRoleService>>>(){}).to(RoleListener.class);
        }

        @Override
        public Key<? extends Service.Listener> getKey() {
            return Key.get(RoleListener.class);
        }
    }
    
    public static final class ListenersFactory implements Function<RegionRole, RegionRoleService> {

        public static ListenersFactory create() {
            return new ListenersFactory(Collections.synchronizedList(Lists.<Function<RegionRole, ? extends Iterable<? extends Service.Listener>>>newLinkedList()));
        }
        
        private final List<Function<RegionRole, ? extends Iterable<? extends Service.Listener>>> registry;
        
        protected ListenersFactory(
                List<Function<RegionRole, ? extends Iterable<? extends Service.Listener>>> registry) {
            this.registry = registry;
        }
        
        public List<Function<RegionRole, ? extends Iterable<? extends Service.Listener>>> get() {
            return registry;
        }
        
        @Override
        public RegionRoleService apply(RegionRole input) {
            ImmutableList.Builder<Service.Listener> listeners = ImmutableList.builder();
            synchronized (registry) {
                for (Function<RegionRole, ? extends Iterable<? extends Service.Listener>> fn: registry) {
                    for (Service.Listener listener: fn.apply(input)) {
                        listeners.add(listener);
                    }
                }
            }
            return RegionRoleService.create(input, listeners.build());
        }
    }

    public static final class RoleListener extends LoggingServiceListener<RoleListener> implements Automatons.AutomatonListener<RegionRole>, Supplier<FutureTransition<RegionRoleService>> {
        
        public static RoleListener create(
                Function<RegionRole, ? extends RegionRoleService> factory,
                Automatons.EventfulAutomaton<RegionRole,?> role) {
            return new RoleListener(factory, role);
        }
        
        private final Automatons.EventfulAutomaton<RegionRole,?> role;
        private final TransitionActor actor;
        
        protected RoleListener(
                Function<RegionRole, ? extends RegionRoleService> factory,
                Automatons.EventfulAutomaton<RegionRole,?> role) {
            this.role = role;
            this.actor = new TransitionActor(factory, logger);
        }
        
        @Override
        public FutureTransition<RegionRoleService> get() {
            return actor.get();
        }

        @Override
        public void handleAutomatonTransition(
            Automaton.Transition<RegionRole> transition) {
            logger.debug("TRANSITION ({}) ({})", delegate(), transition);
            actor.send(transition);
        }
    
        @Override
        public void starting() {
            // replay
            handleAutomatonTransition(Automaton.Transition.create(RegionRole.unknown(), role.state()));
            super.starting();
            role.subscribe(this);
        }
    
        @Override
        public void stopping(Service.State from) {
            super.stopping(from);
            stop();
        }
    
        @Override
        public void failed(Service.State from, Throwable failure) {
            super.failed(from, failure);
            stop();
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).toString();
        }
        
        protected void stop() {
            role.unsubscribe(this);
            actor.stop();
        }
        
        protected static final class TransitionActor extends Actors.QueuedActor<Automaton.Transition<RegionRole>> implements Supplier<FutureTransition<RegionRoleService>> {

            private final Function<RegionRole, ? extends RegionRoleService> factory;
            private FutureTransition<RegionRoleService> player;
            
            protected TransitionActor(
                    Function<RegionRole, ? extends RegionRoleService> factory,
                    Logger logger) {
                super(Queues.<Automaton.Transition<RegionRole>>newConcurrentLinkedQueue(), logger);
                this.factory = factory;
                this.player = FutureTransition.absent(SettableFuturePromise.<RegionRoleService>create());
            }
            
            @Override
            public synchronized FutureTransition<RegionRoleService> get() {
                return player;
            }

            @Override
            public synchronized boolean isReady() {
                if (!super.isReady()) {
                    return false;
                }
                if (player.getCurrent().isPresent()) {
                    RegionRoleService current = player.getCurrent().get();
                    Automaton.Transition<RegionRole> input = mailbox.peek();
                    if (input.to().equals(current.getRole())) {
                        mailbox.remove(input);
                        return isReady();
                    }
                    switch (current.state()) {
                    case TERMINATED:
                    case FAILED:
                        break;
                    default:
                        current.addListener(
                                new Service.Listener() {
                                    @Override
                                    public void terminated(Service.State from) {
                                        run();
                                    }
                                    @Override
                                    public void failed(Service.State from, Throwable t) {
                                        run();
                                    }
                                }, 
                                SameThreadExecutor.getInstance());
                        switch (current.state()) {
                        case TERMINATED:
                        case FAILED:
                            break;
                        default:
                            try {
                                Services.stop(current);
                            } catch (IllegalStateException e) {
                                return true;
                            }
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            protected synchronized boolean apply(Automaton.Transition<RegionRole> input)
                    throws Exception {
                if (state() == State.TERMINATED) {
                    return false;
                }
                FutureTransition<RegionRoleService> previous = player;
                player = FutureTransition.present(factory.apply(input.to()), SettableFuturePromise.<RegionRoleService>create());
                ((Promise<RegionRoleService>) previous.getNext()).set(player.getCurrent().get());
                Services.start(player.getCurrent().get());
                return true;
            }

            @Override
            protected synchronized void doStop() {
                super.doStop();
                if (player.getCurrent().isPresent()) {
                    try {
                        Services.stop(player.getCurrent().get());
                    } catch (IllegalStateException e) {}
                }
                player.getNext().cancel(false);
            }
        }
    }
    
    public static RegionRoleService create(
            RegionRole role,
            Iterable<? extends Service.Listener> listeners) {
        return new RegionRoleService(role, listeners);
    }
    
    private final RegionRole role;
    
    protected RegionRoleService(
            RegionRole role,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.role = role;
    }

    public RegionRole getRole() {
        return role;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(getRole()).toString();
    }
}
