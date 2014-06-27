package edu.uw.zookeeper.safari.cli;


import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.cli.DispatchingInvoker;
import edu.uw.zookeeper.client.cli.Shell;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;

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
        protected MainBuilder newInstance(RuntimeModule runtime) {
            return new MainBuilder(runtime);
        }
        
        @Override
        protected String getDescription() {
            return DESCRIPTION;
        }
        
        @Override
        protected DispatchingInvoker newDispatchingInvoker(Shell shell) {
            return DispatchingInvoker.defaults(shell, ControlInvoker.class);
        }
        
        @Override
        public Main newMain(Shell shell) {
            return new Main(ServiceApplication.forService(getRuntimeModule().getServiceMonitor()));
        }
    }
}
