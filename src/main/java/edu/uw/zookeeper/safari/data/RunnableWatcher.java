package edu.uw.zookeeper.safari.data;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.common.SameThreadExecutor;


public class RunnableWatcher<T extends Runnable> extends AbstractWatchListener implements Runnable {

    public static <T extends Runnable> RunnableWatcher<T> newInstance(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            T runnable) {
        RunnableWatcher<T> instance = new RunnableWatcher<T>(service, watch, matcher, runnable);
        service.addListener(instance, SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            instance.starting();
            instance.running();
        }
        return instance;
    }
    
    protected final T runnable;

    public RunnableWatcher(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            T runnable) {
        super(service, watch, matcher);
        this.runnable = runnable;
    }
    
    public T runnable() {
        return runnable();
    }
    
    @Override
    public void run() {
        runnable.run();
    }
    
    @Override
    public void handleWatchEvent(WatchEvent event) {
        if (service.isRunning()) {
            run();
        }
    }

    @Override
    public void running() {
        run();
    }
}