package edu.uw.zookeeper.orchestra.control;

import edu.uw.zookeeper.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;

public class SimpleControlConnections extends ControlConnectionsService.Module {

    public static SimpleControlConnections create() {
        return new SimpleControlConnections();
    }
    
    protected SimpleControlConnections() {
    }

    @Override
    protected <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super Operation.Request>> ParameterizedFactory<Pair<Pair<Class<I>, T>, C>, ? extends ProtocolCodecConnection<I,T,C>> getConnectionFactory(
            TimeValue timeOut, ListeningExecutorServiceFactory executors) { 
        return protocolCodecConnectionFactory();
    }

    @Override
    protected com.google.inject.Module[] getModules() {
        com.google.inject.Module[] modules = { SimpleControlConfiguration.module()};
        return modules;
    }
}
