package edu.uw.zookeeper.orchestra;

public interface ServiceLocator {
    <T> T getInstance(Class<T> type);
}
