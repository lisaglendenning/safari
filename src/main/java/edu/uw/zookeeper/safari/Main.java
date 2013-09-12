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

    protected static class MainBuilder implements ZooKeeperApplication.RuntimeBuilder<Main, MainBuilder> {
        
        protected final RuntimeModule runtime;
        
        public MainBuilder() {
            this(null);
        }

        public MainBuilder(
                RuntimeModule runtime) {
            this.runtime = runtime;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @Override
        public MainBuilder setRuntimeModule(RuntimeModule runtime) {
            return newInstance(runtime);
        }

        @Override
        public MainBuilder setDefaults() {
            return this;
        }

        @Override
        public Main build() {
            return setDefaults().doBuild();
        }

        protected MainBuilder newInstance(RuntimeModule runtime) {
            return new MainBuilder(runtime);
        }

        protected Main doBuild() {
            return new Main(MainApplicationModule.getApplication(getRuntimeModule()));
        }
    }
}
