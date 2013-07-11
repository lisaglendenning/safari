package edu.uw.zookeeper.orchestra;

import com.google.common.util.concurrent.AbstractIdleService;

public abstract class DependentService extends AbstractIdleService {

    protected DependentService() {}
    
    @Override
    protected void startUp() throws Exception {
        Dependencies.startDependenciesAndWait(this, locator());
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    protected abstract ServiceLocator locator();
    
    public static abstract class SimpleDependentService extends DependentService {

        private final ServiceLocator locator;
        
        protected SimpleDependentService(ServiceLocator locator) {
            this.locator = locator;
        }

        protected ServiceLocator locator() {
            return locator;
        }
    }
}
