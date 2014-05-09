package edu.uw.zookeeper.safari.peer;

import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;

public class RunWhenNotPresent extends Service.Listener implements Runnable {

    public static RunWhenNotPresent create(
            Runnable runnable,
            Identifier peer,
            ControlMaterializerService control,
            Logger logger) {
        return new RunWhenNotPresent(runnable, peer, control, logger);
    }
    
    protected final Runnable runnable;
    protected final PresenceWatcher presence;
    
    protected RunWhenNotPresent(
            Runnable runnable,
            Identifier peer,
            ControlMaterializerService control,
            Logger logger) {
        this.presence = PresenceWatcher.create(
                peer, 
                control, 
                LoggingPromise.create(logger, SettableFuturePromise.<Identifier>create()));
        this.runnable = runnable;
        presence.addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public void run() {
        if (presence.isDone()) {
            try {
                presence.get();
                runnable.run();
            } catch (Exception e) {}
        }
    }
}