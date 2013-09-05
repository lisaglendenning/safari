package edu.uw.zookeeper.orchestra.common;

import java.util.concurrent.Executor;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;

import edu.uw.zookeeper.common.ServiceMonitor;

public abstract class DependentService extends AbstractIdleService {

    protected final Injector injector;
    
    protected DependentService(Injector injector) {
        this.injector = injector;
    }
    
    protected Injector injector() {
        return injector;
    }

    @Override
    protected Executor executor() {
        return MoreExecutors.sameThreadExecutor();
    }
    
    @Override
    protected void startUp() throws Exception {
        injector().getInstance(DependentServiceMonitor.class)
            .start(injector(), getClass().getAnnotation(DependsOn.class));
        injector().getInstance(ServiceMonitor.class).addOnStart(this);
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
