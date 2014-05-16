package edu.uw.zookeeper.safari.backend;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.clients.jmx.ServerViewJmxQuery;
import edu.uw.zookeeper.clients.jmx.SunAttachQueryJmx;
import edu.uw.zookeeper.common.DefaultsFactory;

public class BackendEnsembleViewDiscovery implements Callable<EnsembleView<ServerInetAddressView>> {

    public static EnsembleView<ServerInetAddressView> get() {
        return new BackendEnsembleViewDiscovery().call();
    }
    
    @Override
    public EnsembleView<ServerInetAddressView> call() {        
        DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
        JMXServiceURL url = urlFactory.get();
        JMXConnector connector = null;
        try {
            connector = JMXConnectorFactory.connect(url);
            MBeanServerConnection mbeans = connector.getMBeanServerConnection();
            EnsembleView<ServerRoleView> roles = ServerViewJmxQuery.ensembleViewOf(mbeans);
            if (roles == null) {
                return EnsembleView.copyOf(ServerViewJmxQuery.addressViewOf(mbeans));
            } else {
                return EnsembleView.create(ImmutableSortedSet.copyOf(Iterators.transform(
                        roles.iterator(), 
                        new Function<ServerRoleView, ServerInetAddressView>() {
                            @Override
                            public ServerInetAddressView apply(
                                    ServerRoleView input) {
                                return input.address();
                            }
                        })));
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            try {
                if (connector != null) {
                    connector.close();
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}