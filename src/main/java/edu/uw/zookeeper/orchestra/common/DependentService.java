package edu.uw.zookeeper.orchestra.common;

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class DependentService extends AbstractIdleService {

    protected final ServiceLocator locator;
    
    protected DependentService(ServiceLocator locator) {
        this.locator = locator;
    }
    
    protected ServiceLocator locator() {
        return locator;
    }

    @Override
    protected Executor executor() {
        return MoreExecutors.sameThreadExecutor();
    }
    
    @Override
    protected void startUp() throws Exception {
        locator().getInstance(DependentServiceMonitor.class)
            .start(getClass().getAnnotation(DependsOn.class))
            .get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
