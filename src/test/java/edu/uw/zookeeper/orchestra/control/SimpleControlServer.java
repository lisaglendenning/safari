package edu.uw.zookeeper.orchestra.control;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.server.SimpleServerConnections;
import edu.uw.zookeeper.server.SimpleServerExecutor;

public class SimpleControlServer extends AbstractIdleService {

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
    protected void startUp() throws Exception {
        connections.addListener(new Listener() {

            @Override
            public void starting() {
            }

            @Override
            public void running() {
            }

            @Override
            public void stopping(State from) {
                stop();
            }

            @Override
            public void terminated(State from) {
                stop();
            }

            @Override
            public void failed(State from, Throwable failure) {
                stop();
            }}, MoreExecutors.sameThreadExecutor());
        connections.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        connections.stop().get();
    }
}
