package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.server.ServerApplicationModule;

public abstract class FrontendConfiguration {
    public static ServerInetAddressView get(RuntimeModule runtime) {
         return ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
    }
}
