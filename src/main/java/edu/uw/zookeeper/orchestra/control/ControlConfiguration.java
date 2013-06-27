package edu.uw.zookeeper.orchestra.control;

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.Map;

import com.typesafe.config.Config;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factories;

public class ControlConfiguration extends Factories.Holder<EnsembleRoleView<InetSocketAddress, ServerInetAddressView>> {
    
    public static ControlConfiguration fromRuntime(RuntimeModule runtime) {
        return new ControlConfiguration(factory().get(runtime.configuration()));
    }
    
    public ControlConfiguration(
            EnsembleRoleView<InetSocketAddress, ServerInetAddressView> instance) {
        super(instance);
    }

    public static ControlEnsembleViewFactory factory() {
        return ControlEnsembleViewFactory.getInstance();
    }

    public static final String ARG = "control";
    public static final String CONFIG_KEY = "Control";

    public static final String DEFAULT_ADDRESS = "localhost";
    public static final int DEFAULT_PORT = 2381;
    
    public static final String CONFIG_PATH = "";
    
    public static enum ControlEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleRoleView<InetSocketAddress, ServerInetAddressView>> {
        INSTANCE;
        
        public static ControlEnsembleViewFactory getInstance() {
            return INSTANCE;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public EnsembleRoleView<InetSocketAddress, ServerInetAddressView> get() {
            return EnsembleRoleView.ofRoles(
                    ServerRoleView.of(ServerInetAddressView.of(
                    DEFAULT_ADDRESS, DEFAULT_PORT)));
        }
    
        @Override
        public EnsembleRoleView<InetSocketAddress, ServerInetAddressView> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Ensemble"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(CONFIG_PATH, args);
            if (config.hasPath(CONFIG_KEY)) {
                String input = config.getString(CONFIG_KEY);
                return EnsembleRoleView.fromStringRoles(input);
            } else {
                return get();
            }
        }
    }
}
