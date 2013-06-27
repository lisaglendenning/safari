package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.server.ServerApplicationModule;

public class ConductorConfiguration {
    public static ConductorAddressView get(ServiceManager manager) throws InterruptedException, ExecutionException, KeeperException {
        Materializer materializer = manager.controlClient().materializer();
        ServerInetAddressView conductorAddress = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance(
                        "conductorAddress", "ConductorAddress", "", "", 2281).get(manager.runtime().configuration());
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.create(conductorAddress, materializer);
        return ConductorAddressView.of(entityNode.get(), conductorAddress);
    }
}
