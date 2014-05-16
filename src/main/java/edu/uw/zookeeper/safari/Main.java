package edu.uw.zookeeper.safari;


import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;

public class Main extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected Main(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.AbstractRuntimeBuilder<Main, MainBuilder> {

        protected static final String DESCRIPTION = "ZooKeeper Safari Server";
        
        public MainBuilder() {
            this(null);
        }

        public MainBuilder(
                RuntimeModule runtime) {
            super(runtime);
        }

        @Override
        protected MainBuilder newInstance(RuntimeModule runtime) {
            return new MainBuilder(runtime);
        }

        @Override
        protected Main doBuild() {
            getRuntimeModule().getConfiguration().getArguments().setDescription(getDescription());
            return new Main(Module.createInjector(getRuntimeModule()).getInstance(Application.class));
        }
        
        protected String getDescription() {
            return DESCRIPTION;
        }
    }
}
