package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.orchestra.common.DependentModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;

public abstract class ClientConnectionsModule extends DependentModule {
    
    protected ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> getCodecFactory() {
        return AssignXidCodec.factory();
    }

    protected <I extends Operation.Request, T extends ProtocolCodec<?, ?>, C extends Connection<? super Operation.Request>> ParameterizedFactory<Pair<Pair<Class<I>, T>, C>, ? extends ProtocolCodecConnection<I,T,C>> getConnectionFactory(
            TimeValue timeOut, ListeningExecutorServiceFactory executors) { 
        return PingingClient.factory(timeOut, executors.asListeningScheduledExecutorServiceFactory().get());
    }
    
    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(
            TimeValue timeOut,
            ListeningExecutorServiceFactory executors,
            NetClientModule clients) {
        return clients.getClientConnectionFactory(
                    getCodecFactory(), 
                    this.<Operation.Request, AssignXidCodec, Connection<Operation.Request>>getConnectionFactory(timeOut, executors)).get();
    }

    protected ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(
            NetClientModule clients) {
        return clients.getClientConnectionFactory(
                    getCodecFactory(), 
                    ProtocolCodecConnection.<Operation.Request,AssignXidCodec,Connection<Operation.Request>>factory()).get();
    }
}
