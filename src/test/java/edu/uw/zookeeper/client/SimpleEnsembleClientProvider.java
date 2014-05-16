package edu.uw.zookeeper.client;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.EnsembleViewConfiguration;
import edu.uw.zookeeper.common.RuntimeModule;

public class SimpleEnsembleClientProvider implements Provider<ConnectionClientExecutorService.Builder> {
    
    protected final EnsembleView<ServerInetAddressView> ensemble;
    protected final RuntimeModule runtime;
    protected final Provider<ConnectionClientExecutorService.Builder> delegate;

    @Inject
    public SimpleEnsembleClientProvider(
            EnsembleView<ServerInetAddressView> ensemble,
            RuntimeModule runtime,
            Injector injector) {
        this(ensemble, runtime, injector, SimpleClientProvider.class);
    }
    
    protected SimpleEnsembleClientProvider(
            EnsembleView<ServerInetAddressView> ensemble,
            RuntimeModule runtime,
            Injector injector,
            Class<? extends Provider<ConnectionClientExecutorService.Builder>> delegate) {
        this(ensemble, runtime, injector.getInstance(delegate));
    }
    
    protected SimpleEnsembleClientProvider(
            EnsembleView<ServerInetAddressView> ensemble,
            RuntimeModule runtime,
            Provider<ConnectionClientExecutorService.Builder> delegate) {
        this.delegate = delegate;
        this.runtime = runtime;
        this.ensemble = ensemble;
    }
    
    @Override
    public ConnectionClientExecutorService.Builder get() {
        EnsembleViewConfiguration.set(runtime.getConfiguration(), ensemble);
        return delegate.get();
    }
}
