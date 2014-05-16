package edu.uw.zookeeper.safari.storage;

import java.lang.annotation.Annotation;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.safari.AbstractSafariModule;

public class Module extends AbstractSafariModule {

    public static Class<Storage> annotation() {
        return Storage.class;
    }
    
    public static Module create() {
        return new Module();
    }
    
    public Module() {}

    @Override
    public Class<? extends Annotation> getAnnotation() {
        return annotation();
    }

    @Override
    protected ImmutableList<? extends com.google.inject.Module> getModules() {
        return ImmutableList.<com.google.inject.Module>of(
                StorageCfgFile.module(),
                StorageEnsembleConnections.create(),
                StorageServerConnections.create(),
                StorageClientService.module());
    }

    @Override
    protected Class<? extends Service> getServiceType() {
        return StorageClientService.class;
    }
}
