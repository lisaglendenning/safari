package edu.uw.zookeeper.safari.storage.volumes;

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
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class VolumeWatchers extends AbstractModule implements SafariModule {

    public static VolumeWatchers create() {
        return new VolumeWatchers();
    }
        
    protected VolumeWatchers() {}
    
    @Override
    public Key<? extends Service> getKey() {
        return Key.get(new TypeLiteral<DirectoryWatcherService<StorageSchema.Safari.Volumes>>(){});
    }

    @Provides @Singleton
    public DirectoryWatcherService<StorageSchema.Safari.Volumes> getVolumesWatcherService(
            final Injector injector,
            final SchemaClientService<StorageZNode<?>,?> client,
            final ServiceMonitor monitor) {
        DirectoryWatcherService<StorageSchema.Safari.Volumes> instance = DirectoryWatcherService.listen(
                StorageSchema.Safari.Volumes.class,
                client,
                ImmutableList.of(
                        new Service.Listener() {
                            @Override
                            public void starting() {
                                injector.getInstance(
                                        Key.get(new TypeLiteral<DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume>>(){}));
                            }
                        }));
        monitor.add(instance);
        return instance;
    }

    @Provides @Singleton
    public DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume.Log.Version> getVolumeVersionListener(
            final SchemaClientService<StorageZNode<?>,?> client,
            final DirectoryWatcherService<StorageSchema.Safari.Volumes> service) {
        final VolumeVersionListener instance = VolumeVersionListener.create(
                client, 
                ImmutableList.<WatchMatchListener>of(),
                service,
                LogManager.getLogger(StorageSchema.Safari.Volumes.Volume.Log.Version.class));
        instance.listen();
        return instance;
    }
    
    @Provides @Singleton
    public DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume> getVolumeListener(
            final DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            final SchemaClientService<StorageZNode<?>,?> client,
            final DirectoryWatcherService<StorageSchema.Safari.Volumes> service) {
        final DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume> instance = DirectoryEntryListener.create(
                StorageSchema.Safari.Volumes.Volume.class,
                client,
                service,
                LogManager.getLogger(StorageSchema.Safari.Volumes.Volume.class));
        instance.listen();
        return instance;
    }

    @Override
    protected void configure() {
    }
}
