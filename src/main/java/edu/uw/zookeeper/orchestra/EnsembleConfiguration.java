package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.orchestra.control.Orchestra;

public class EnsembleConfiguration {
    public static Identifier get(ServiceManager manager) throws InterruptedException, ExecutionException, KeeperException {
        // Find my ensemble
        EnsembleView<ServerInetAddressView> myView = manager.backendClient().view().getEnsemble();
        Orchestra.Ensembles.Entity ensembleNode = Orchestra.Ensembles.Entity.create(myView, manager.controlClient().materializer());
        return ensembleNode.get();
    }
}
