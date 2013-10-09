package edu.uw.zookeeper.safari.common;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.protocol.Operation;

public interface OperationFuture<V extends Operation.ProtocolResponse<?>> extends ListenableFuture<V>, Stateful<OperationFuture.State>, Callable<OperationFuture.State>, Operation.RequestId {

    public static enum State {
        WAITING, SUBMITTING, COMPLETE, PUBLISHED;
    }
}