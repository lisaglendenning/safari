package edu.uw.zookeeper.safari.control;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;

public class ControlEnsembleConnections extends AbstractModule {

    public static ControlEnsembleConnections create() {
        return new ControlEnsembleConnections();
    }
    
    protected ControlEnsembleConnections() {}
    
    @Override
    protected void configure() {
    }

    @Provides @Control @Singleton
    public EnsembleView<ServerInetAddressView> getControlEnsembleConfiguration(
            Configuration configuration) {
        return ControlEnsembleViewConfiguration.get(configuration);
    }

    @Provides @Control @Singleton
    public TimeValue getControlTimeOutConfiguration(Configuration configuration) {
        return ControlTimeOutConfiguration.get(configuration);
    }
    
    @Provides @Control @Singleton
    public ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> newClientConnectionFactory(                
            RuntimeModule runtime,
            @Control TimeValue timeOut,
            NetClientModule clientModule) {
        ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> instance = ClientConnectionFactoryBuilder.defaults()
                .setClientModule(clientModule)
                .setTimeOut(timeOut)
                .setRuntimeModule(runtime)
                .build();
        runtime.getServiceMonitor().add(instance);
        return instance;
    }

    @Provides @Control @Singleton
    public EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> newControlClientFactory(
            @Control EnsembleView<ServerInetAddressView> ensemble,
            @Control TimeValue timeOut,
            @Control ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
            ScheduledExecutorService scheduler) {
        EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> factory = EnsembleViewFactory.fromSession(
                connections,
                ensemble, 
                timeOut,
                scheduler);
        return factory;
    }
    
    @Configurable(path="control", key="ensemble", arg="control", value="127.0.0.1:2381", help="address:port,...")
    public static abstract class ControlEnsembleViewConfiguration {

        public static Configurable getConfigurable() {
            return ControlEnsembleViewConfiguration.class.getAnnotation(Configurable.class);
        }
        
        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            String value = 
                    configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.key());
            return ServerInetAddressView.ensembleFromString(value);
        }
        
        public static Configuration set(Configuration configuration, EnsembleView<ServerInetAddressView> value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(
                    ConfigFactory.parseMap(
                            ImmutableMap.<String,Object>builder()
                            .put(ConfigUtil.joinPath(configurable.path(), configurable.key()), 
                                    EnsembleView.toString(value)).build()));
        }
        
        protected ControlEnsembleViewConfiguration() {}
    }

    @Configurable(path="control", key="timeout", value="30 seconds", help="time")
    public static abstract class ControlTimeOutConfiguration {

        public static TimeValue get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                    .getString(configurable.key()));
        }
        
        public static Configuration set(Configuration configuration, TimeValue value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(
                    ConfigFactory.parseMap(
                            ImmutableMap.<String,Object>builder()
                            .put(ConfigUtil.joinPath(configurable.path(), configurable.key()), 
                                    value.toString()).build()));
        }
        
        public static Configurable getConfigurable() {
            return ControlTimeOutConfiguration.class.getAnnotation(Configurable.class);
        }
        
        protected ControlTimeOutConfiguration() {}
    }
}