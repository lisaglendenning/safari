package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.util.Factories;

public class ConductorConfiguration extends Factories.Holder<ConductorAddressView> {

    public static ConductorConfiguration fromRuntime(ControlClientService controlClient, RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
        Materializer materializer = controlClient.materializer();
        ServerInetAddressView conductorAddress = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance(
                        "conductorAddress", "ConductorAddress", "", "", 2281).get(runtime.configuration());
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.create(conductorAddress, materializer);
        return new ConductorConfiguration(ConductorAddressView.of(entityNode.get(), conductorAddress));
    }

    public ConductorConfiguration(ConductorAddressView instance) {
        super(instance);
    }
}
