package edu.uw.zookeeper.safari.region;

import java.lang.annotation.Annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.safari.AbstractSafariModule;

public class Module extends AbstractSafariModule {

    public static Class<Region> annotation() {
        return Region.class;
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
                RegionConfiguration.create(),
                RegionLeaderService.module(),
                RegionPlayerService.module(),
                RegionMemberService.module());
    }

    @Override  
    protected Class<? extends Service> getServiceType() {
        return RegionMemberService.class;
    }
}
