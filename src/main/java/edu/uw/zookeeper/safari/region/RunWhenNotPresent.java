package edu.uw.zookeeper.safari.region;

import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;

public class RunWhenNotPresent extends Service.Listener implements Runnable {

    public static RunWhenNotPresent create(
            Runnable runnable,
            Identifier peer,
            ControlClientService control,
            Logger logger) {
        PresenceWatcher presence = PresenceWatcher.create(
                peer, 
                control, 
                LoggingPromise.create(logger, SettableFuturePromise.<Identifier>create()));
        return new RunWhenNotPresent(logger, presence, runnable);
    }
    
    protected final Logger logger;
    protected final Runnable runnable;
    protected final PresenceWatcher presence;
    
    protected RunWhenNotPresent(
            Logger logger,
            PresenceWatcher presence,
            Runnable runnable) {
        this.logger = logger;
        this.presence = presence;
        this.runnable = runnable;
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