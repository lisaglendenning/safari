package edu.uw.zookeeper.common;

import java.util.concurrent.Callable;


public class Call<T extends Callable<?>> implements Runnable {

    public static <T extends Callable<?>> Call<T> create(T callable) {
        return new Call<T>(callable);
    }
    
    protected final T callable;
    
    protected Call(
            T callable) {
        this.callable = callable;
    }
    
    public T callable() {
        return callable;
    }
    
    @Override
    public void run() {
        try {
            callable.call();
        } catch (Exception e) {
        }
    }
}