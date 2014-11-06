package edu.uw.zookeeper.safari.frontend;

import org.apache.logging.log4j.LogManager;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class RegionWatchers extends AbstractModule implements SafariModule {

    public static RegionWatchers create() {
        return new RegionWatchers();
    }
        
    protected RegionWatchers() {}

    @Override  
    public Key<? extends Service> getKey() {
        return Key.get(new TypeLiteral<DirectoryWatcherService<ControlSchema.Safari.Regions>>(){});
    }
    
    @Provides @Singleton
    public DirectoryWatcherService<ControlSchema.Safari.Regions> getRegionsWatcherService(
            final Injector injector,
            final SchemaClientService<ControlZNode<?>,?> client,
            final ServiceMonitor monitor) {
        DirectoryWatcherService<ControlSchema.Safari.Regions> instance = DirectoryWatcherService.listen(
                ControlSchema.Safari.Regions.class,
                client,
                ImmutableList.of(
                        new Service.Listener() {
                            @Override
                            public void starting() {
                                injector.getInstance(
                                        Key.get(new TypeLiteral<DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Regions.Region>>(){}));
                            }
                        }));
        monitor.add(instance);
        return instance;
    }
    
    @Provides @Singleton
    public static DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Regions.Region> getRegionListener(
            SchemaClientService<ControlZNode<?>,?> client,
            DirectoryWatcherService<ControlSchema.Safari.Regions> service) {
        final DirectoryEntryListener<ControlZNode<?>,ControlSchema.Safari.Regions.Region> instance = DirectoryEntryListener.create(
                ControlSchema.Safari.Regions.Region.class,
                client,
                service,
                LogManager.getLogger(ControlSchema.Safari.Regions.Region.class));
        instance.listen();
        return instance;
    }

    @Override
    protected void configure() {
    }
}
