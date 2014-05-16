package edu.uw.zookeeper.safari;

import java.lang.annotation.Annotation;

import com.google.inject.Module;

public interface SafariModule extends Module {
    public Class<? extends Annotation> getAnnotation();
}
