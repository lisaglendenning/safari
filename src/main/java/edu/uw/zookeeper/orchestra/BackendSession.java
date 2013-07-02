package edu.uw.zookeeper.orchestra;

import java.util.List;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.protocol.MessageSessionReply;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyMessage;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.TaskExecutor;

public class BackendSession implements Reference<ClientProtocolExecutor>, TaskExecutor<Operation.SessionRequest, Operation.SessionResult> {

    public static enum PathsOfRequest implements Function<Operation.Request, List<ZNodeLabel.Path>> {
        INSTANCE;

        public static PathsOfRequest getInstance() {
            return INSTANCE;
        }
        
        @Override
        public List<ZNodeLabel.Path> apply(Operation.Request input) {
            ImmutableList.Builder<ZNodeLabel.Path> paths = ImmutableList.builder();
            if (input instanceof IMultiRequest) {
                for (Records.MultiOpRequest e: (IMultiRequest)input) {
                    paths.addAll(apply(e));
                }
            } else if (input instanceof Records.PathHolder) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathHolder) input).getPath());
                paths.add(path);
            }
            return paths.build();
        }
    }
    
    
    public static BackendSession newInstance(
            long sessionId,
            Connection<MessagePacket> handler,
            ClientProtocolExecutor client,
            ActionVolumeTranslator translator) {
        return new BackendSession(sessionId, handler, client, translator);
    }
    
    protected final long sessionId;
    protected final ClientProtocolExecutor client;
    protected final ActionVolumeTranslator translator;
    protected final Connection<MessagePacket> handler;
    
    protected BackendSession(
            long sessionId,
            Connection<MessagePacket> handler,
            ClientProtocolExecutor client,
            ActionVolumeTranslator translator) {
        this.sessionId = sessionId;
        this.client = client;
        this.translator = translator;
        this.handler = handler;
        
        client.register(this);
        client.connect();
    }
    
    public long sessionId() {
        return sessionId;
    }
    
    @Subscribe
    public void handleSessionReply(Operation.SessionReply event) {
        Operation.SessionReply translated = SessionReplyMessage.newInstance(
                event.xid(), 
                event.zxid(), 
                (Operation.Response) translator.apply(event.reply()));
        handler.write(MessagePacket.of(MessageSessionReply.of(sessionId(), translated)));
    }

    @Override
    public ClientProtocolExecutor get() {
        return client;
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(
            Operation.SessionRequest request) {
        return submit(
                request, 
                SettableFuturePromise.<Operation.SessionResult>create());
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(
            Operation.SessionRequest request,
            Promise<Operation.SessionResult> promise) {
        Operation.SessionRequest translated = SessionRequestMessage.newInstance(
                request.xid(), 
                (Operation.Request) translator.apply(request.request()));
        return get().submit(translated, promise);
    }
}
