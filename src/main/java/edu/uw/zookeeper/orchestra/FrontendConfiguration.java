package edu.uw.zookeeper.orchestra;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.util.Factories;

public class FrontendConfiguration extends Factories.Holder<ServerInetAddressView> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public FrontendConfiguration getFrontendConfiguration(RuntimeModule runtime) {
            ServerInetAddressView view = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(runtime.configuration());
            return new FrontendConfiguration(view);
        }
    }

    public FrontendConfiguration(ServerInetAddressView instance) {
        super(instance);
    }    
}
