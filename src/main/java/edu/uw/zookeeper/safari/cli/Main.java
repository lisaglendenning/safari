package edu.uw.zookeeper.safari.cli;


import java.io.IOException;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.cli.DispatchingInvoker;
import edu.uw.zookeeper.client.cli.Shell;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class Main extends edu.uw.zookeeper.client.cli.Main {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected Main(Application delegate) {
        super(delegate);
    }
    
    protected static class MainBuilder extends edu.uw.zookeeper.client.cli.Main.MainBuilder {

    	protected static final String DESCRIPTION = "ZooKeeper Safari CLI Client\nType '?' at the prompt to get started.";
    	
        public MainBuilder() {
            this(null);
        }
        
        protected MainBuilder(RuntimeModule runtime) {
            super(runtime);
        }

        @Override
        public MainBuilder setRuntimeModule(RuntimeModule runtime) {
            return (MainBuilder) super.setRuntimeModule(runtime);
        }

        @Override
        public MainBuilder setDefaults() {
            return (MainBuilder) super.setDefaults();
        }

        @Override
        public Main build() {
            return (Main) super.build();
        }

        @Override
        protected Main doBuild() {
            getRuntimeModule().getConfiguration().getArguments().setDescription(DESCRIPTION);
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            Shell shell;
            try {
                shell = Shell.create(runtime);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            monitor.add(shell);
            monitor.add(DispatchingInvoker.defaults(shell, ControlInvoker.class));
            return new Main(ServiceApplication.newInstance(monitor));
        }

        @Override
        protected MainBuilder newInstance(RuntimeModule runtime) {
            return new MainBuilder(runtime);
        }
    }
}
