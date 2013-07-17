package edu.uw.zookeeper.orchestra.frontend;

import java.util.Map;
import java.util.concurrent.Executor;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.VolumeAssignment;
import edu.uw.zookeeper.orchestra.backend.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.protocol.proto.Records.Response;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.TaskExecutor;

public class FrontendSessionExecutor extends AbstractActor<FrontendSessionExecutor.RequestTask> implements TaskExecutor<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {

    protected final long sessionId;

    protected FrontendSessionExecutor(
            long sessionId,
            Executor executor) {
        super(executor, FutureQueue.<RequestTask>create(), AbstractActor.newState());
        this.sessionId = sessionId;
    }
    
    @Subscribe
    public void handleResponse(ShardedResponseMessage<?> response) {
        
    }

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            Message.ClientRequest<Records.Request> request) {
        ListenableFuture<Map<ZNodeLabel.Path, VolumeAssignment>> lookup = LookupRequests.lookup(request.getRecord(), null);
        Promise<Message.ServerResponse<Response>> promise = SettableFuturePromise.create();
        RequestTask task = new RequestTask(request, promise);
        send(task);
        return task;
    }

    protected class RequestTask extends PromiseTask<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {

        public RequestTask(
                Message.ClientRequest<Request> task,
                Promise<Message.ServerResponse<Response>> delegate) {
            super(task, delegate);
        }
        
        public void onSuccess(Map<ZNodeLabel.Path, VolumeAssignment> result) {
            switch (task().getRecord().getOpcode()) {
            default:
                break;
            }
        }
    }
}
