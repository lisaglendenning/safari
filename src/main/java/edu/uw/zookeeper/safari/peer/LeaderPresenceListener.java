package edu.uw.zookeeper.safari.peer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;

public class LeaderPresenceListener extends AbstractRoleListener {

    protected static LeaderPresenceListener create(
            Runnable runnable,
            Automatons.EventfulAutomaton<RegionRole,?> role,
            Service service,
            ControlMaterializerService control) {
        return new LeaderPresenceListener(runnable, control, role, service);
    }
    
    protected final Logger logger;
    protected final ControlMaterializerService control;
    protected final Runnable runnable;
    
    protected LeaderPresenceListener(
            Runnable runnable,
            ControlMaterializerService control,
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
            RunWhenNotPresent.create(
                    new Runnable() {
                        @Override
                            public void run() {
                            if (service.isRunning()) {
                                runnable.run();
                            }
                        }
                    },
                    transition.to().getLeader().getLeader(),
                    control,
                    logger);
            break;
        }
        default:
        {
            break;
        }
        }
    }
}