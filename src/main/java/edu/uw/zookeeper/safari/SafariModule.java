package edu.uw.zookeeper.safari;

import com.google.inject.Key;
import com.google.inject.Module;

public interface SafariModule extends Module {
    public Key<?> getKey();
}
