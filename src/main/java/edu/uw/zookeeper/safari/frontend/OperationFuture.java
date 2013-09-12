package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;

public interface OperationFuture<V extends Message.ServerResponse<?>> extends ListenableFuture<V>, Stateful<OperationFuture.State>, Callable<OperationFuture.State>, Operation.RequestId {

    public static enum State {
        WAITING, SUBMITTING, COMPLETE, PUBLISHED;
    }
}