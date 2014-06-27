package edu.uw.zookeeper.safari.region;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;

public class LeaderPresenceListener extends AbstractRoleListener {

    protected static LeaderPresenceListener create(
            Runnable runnable,
            Automatons.EventfulAutomaton<RegionRole,?> role,
            Service service,
            ControlClientService control) {
        return new LeaderPresenceListener(runnable, control, role, service);
    }
    
    protected final Logger logger;
    protected final ControlClientService control;
    protected final Runnable runnable;
    
    protected LeaderPresenceListener(
            Runnable runnable,
            ControlClientService control,
            Automatons.EventfulAutomaton<RegionRole,?> role,
            Service service) {
        super(role, service);
        this.logger = LogManager.getLogger(this);
        this.control = control;
        this.runnable = runnable;
        service.addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public void handleAutomatonTransition(
            Automaton.Transition<RegionRole> transition) {
        switch (transition.to().getRole()) {
        case FOLLOWING:
        {
            final PresenceWatcher presence = PresenceWatcher.create(
                    transition.to().getLeader().getLeader(), 
                    control, 
                    SettableFuturePromise.<Identifier>create());
            LoggingFutureListener.listen(logger, presence);
            new RunWhenNotPresent(presence);
            break;
        }
        default:
        {
            break;
        }
        }
    }
    
    public class RunWhenNotPresent implements Runnable {
        
        protected final PresenceWatcher presence;
        
        public RunWhenNotPresent(
                PresenceWatcher presence) {
            this.presence = presence;
            presence.addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public void run() {
            if (presence.isDone()) {
                try {
                    Identifier peer = presence.get();
                    logger.info("EXPIRED {}", peer);
                    runnable.run();
                } catch (Exception e) {}
            }
        }
    }
}