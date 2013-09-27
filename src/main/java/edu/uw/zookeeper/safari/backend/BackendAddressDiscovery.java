package edu.uw.zookeeper.safari.backend;

import java.io.IOException;
import java.util.concurrent.Callable;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.clients.jmx.ServerViewJmxQuery;
import edu.uw.zookeeper.clients.jmx.SunAttachQueryJmx;
import edu.uw.zookeeper.common.DefaultsFactory;

public class BackendAddressDiscovery implements Callable<ServerInetAddressView> {
    
    public static ServerInetAddressView get() throws IOException {
        return new BackendAddressDiscovery().call();
    }
    
    public static ServerInetAddressView query() throws IOException {
        DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
        JMXServiceURL url = urlFactory.get();
        JMXConnector connector = null;
        try {
            connector = JMXConnectorFactory.connect(url);
            MBeanServerConnection mbeans = connector.getMBeanServerConnection();
            return ServerViewJmxQuery.addressViewOf(mbeans);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            if (connector != null) {
                connector.close();
            }
        }
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
    
    public BackendAddressDiscovery() {
    }
    
    @Override
    public ServerInetAddressView call() throws IOException {
        // If the backend server is not actively serving (i.e. in leader election),
        // then it doesn't advertise it's clientAddress over JMX
        // So poll until I can discover the backend client address
        ServerInetAddressView backend = null;
        long backoff = 1000;
        while (backend == null) {
            backend = query();
            if (backend == null) {
                logger.debug("Querying backend failed; retrying in {} ms", backoff);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                backoff *= 2;
            }
        }
        return backend;
    }
}