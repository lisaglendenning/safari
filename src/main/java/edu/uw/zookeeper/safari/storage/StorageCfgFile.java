package edu.uw.zookeeper.safari.storage;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import edu.uw.zookeeper.Cfg;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(path="storage", key="cfg", arg="storageCfg", help="zoo.cfg")
public class StorageCfgFile implements Supplier<Optional<Properties>> {
    
    public static com.google.inject.Module module() {
        return Module.create();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public StorageCfgFile getStorageCfgFile(
                Configuration configuration) {
            return fromConfiguration(configuration);
        }
    }
    
    public static StorageCfgFile fromConfiguration(Configuration configuration) {
        Configurable configurable = StorageCfgFile.class.getAnnotation(Configurable.class);
        Config config = configuration.withConfigurable(configurable)
                .getConfigOrEmpty(configurable.path());
        Optional<Properties> value;
        if (config.hasPath(configurable.key())) {
            try {
                value = Optional.of(Cfg.load(config.getString(configurable.key())));
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            value = Optional.absent();
        }     
        return new StorageCfgFile(value);
    }
    
    public static StorageCfgFile absent() {
        return new StorageCfgFile(Optional.<Properties>absent());
    }
    
    public static StorageCfgFile of(Properties properties) {
        return new StorageCfgFile(Optional.of(properties));
    }
    
    private final Optional<Properties> value;
    
    protected StorageCfgFile(Optional<Properties> value) {
        this.value = value;
    }
    
    @Override
    public Optional<Properties> get() {
        return value;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(value).toString();
    }
}