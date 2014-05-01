package edu.uw.zookeeper.safari.data;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.common.SameThreadExecutor;


public class FixedQueryWatcher<O extends Operation.ProtocolResponse<?>> extends AbstractWatchListener implements Runnable {

    public static <O extends Operation.ProtocolResponse<?>> FixedQueryWatcher<O> newInstance(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            FixedQuery<O> query) {
        FixedQueryWatcher<O> instance = new FixedQueryWatcher<O>(service, watch, matcher, query);
        service.addListener(instance, SameThreadExecutor.getInstance());
        if (service.isRunning()) {
            instance.starting();
            instance.running();
        }
        return instance;
    }
    
    protected final FixedQuery<O> query;

    public FixedQueryWatcher(
            Service service,
            WatchListeners watch,
            WatchMatcher matcher,
            FixedQuery<O> query) {
        super(service, watch, matcher);
        this.query = query;
    }
    
    public FixedQuery<O> query() {
        return query();
    }
    
    @Override
    public void run() {
        query.call();
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