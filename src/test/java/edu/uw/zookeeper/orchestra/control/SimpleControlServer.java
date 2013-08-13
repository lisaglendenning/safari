package edu.uw.zookeeper.orchestra.control;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.orchestra.common.ForwardingService;
import edu.uw.zookeeper.server.SimpleServerConnections;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleControlServer extends ForwardingService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {
    
        public Module() {
        }
    
        @Override
        protected void configure() {
        }
        
        @Provides @Singleton
        public SimpleControlServer getSimpleControlServer() {
            return SimpleControlServer.create();
        }
    }
    
    public static SimpleControlServer create() {
        SimpleServerExecutor executor = SimpleServerExecutor.newInstance();
        SimpleServerConnections connections = SimpleServerConnections.newInstance(executor.getTasks());
        return new SimpleControlServer(connections, executor);
    }
    
    protected final SimpleServerConnections connections;
    protected final SimpleServerExecutor executor;
    
    protected SimpleControlServer(
            SimpleServerConnections connections,
            SimpleServerExecutor executor) {
        this.connections = connections;
        this.executor = executor;
    }
    
    public SimpleServerConnections getConnections() {
        return connections;
    }
    
    public SimpleServerExecutor getExecutor() {
        return executor;
    }

    @Override
    protected Service delegate() {
        return connections;
    }
}
