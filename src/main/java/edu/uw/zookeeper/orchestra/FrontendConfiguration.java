package edu.uw.zookeeper.orchestra;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.util.Factories;

public class FrontendConfiguration extends Factories.Holder<ServerInetAddressView> {

    public static FrontendConfiguration fromRuntime(RuntimeModule runtime) {
         return new FrontendConfiguration(ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration()));
    }
    
    public FrontendConfiguration(ServerInetAddressView instance) {
        super(instance);
    }    
}
