package edu.uw.zookeeper.orchestra.common;

public interface ServiceLocator {
    <T> T getInstance(Class<T> type);
}
