package edu.uw.zookeeper.net;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Singleton;
import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;

public class IntraVmAsNetModule extends IntraVmModule {

    public static IntraVmAsNetModule create() {
        return new IntraVmAsNetModule();
    }
    
    @Override
    protected void configure() {
        bind(NetClientModule.class).to(IntraVmNetModule.class).in(Singleton.class);
        bind(NetServerModule.class).to(IntraVmNetModule.class).in(Singleton.class);
    }

    @Override
    protected Factory<? extends Executor> newExecutor() {
        return Factories.singletonOf(
                MoreExecutors.getExitingExecutorService(
                        (ThreadPoolExecutor) Executors.newFixedThreadPool(
                                1,
                                new ThreadFactoryBuilder()
                                .setThreadFactory(DefaultRuntimeModule.PlatformThreadFactory.getInstance().get())
                                .setNameFormat("intravm-%d").build())));
    }
}
