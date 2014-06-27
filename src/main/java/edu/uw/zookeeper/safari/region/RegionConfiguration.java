package edu.uw.zookeeper.safari.region;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.CreateEntity;
import edu.uw.zookeeper.safari.storage.Storage;

public class RegionConfiguration extends AbstractModule {

    public static RegionConfiguration create() {
        return new RegionConfiguration();
    }
    
    protected RegionConfiguration() {}

    @Override
    protected void configure() {
    }

    @Provides @Region @Singleton
    public Identifier getRegionIdConfiguration(
            @Storage EnsembleView<ServerInetAddressView> ensemble,
            ControlClientService control) throws InterruptedException, ExecutionException, KeeperException {
        if (!control.isRunning()) {
            control.startAsync().awaitRunning();
        }
        Identifier id = CreateEntity.sync(
                ensemble, 
                ControlSchema.Safari.Regions.class,
                control.materializer()).get();
        LogManager.getLogger(Region.class).info("Region with ensemble {} is {}", ensemble, id);
        return id;
    }
}
