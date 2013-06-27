package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.util.Factories;

public class EnsembleConfiguration extends Factories.Holder<Identifier> {
    
    public static EnsembleConfiguration fromRuntime(
            BackendConfiguration backendConfiguration,
            ControlClientService controlClient) throws InterruptedException, ExecutionException, KeeperException {
        // Find my ensemble
        EnsembleView<ServerInetAddressView> myView = backendConfiguration.get().getEnsemble();
        Orchestra.Ensembles.Entity ensembleNode = Orchestra.Ensembles.Entity.create(myView, controlClient.materializer());
        return new EnsembleConfiguration(ensembleNode.get());
    }

    public EnsembleConfiguration(Identifier instance) {
        super(instance);
    }
}
