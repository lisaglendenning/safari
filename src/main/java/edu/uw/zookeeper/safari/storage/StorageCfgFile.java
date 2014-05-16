package edu.uw.zookeeper.safari.storage;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

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
                FileInputStream cfg = new FileInputStream(
                            new File(config.getString(configurable.key())));
                Properties properties = new Properties();
                try {
                    properties.load(cfg);
                    value = Optional.of(properties);
                } finally {
                    cfg.close();
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(config.getString(configurable.key()));
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
        return Objects.toStringHelper(this).addValue(value).toString();
    }
}