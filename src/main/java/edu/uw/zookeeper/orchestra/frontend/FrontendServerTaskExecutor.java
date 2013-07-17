package edu.uw.zookeeper.orchestra.frontend;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.server.ConnectTableProcessor;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.SessionTable;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.TaskExecutor;

public class FrontendServerTaskExecutor extends ServerTaskExecutor {

    public static FrontendServerTaskExecutor newInstance(
            SessionTable sessions,
            Generator<Long> zxids) {
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(
                        FourLetterRequestProcessor.getInstance());
        ConnectTaskExecutor connectExecutor = new ConnectTaskExecutor(
                ConnectTableProcessor.create(sessions, zxids));
        TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> sessionExecutor = null;
        return new FrontendServerTaskExecutor(
                anonymousExecutor,
                connectExecutor,
                sessionExecutor);
    }
    
    protected FrontendServerTaskExecutor(
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        super(anonymousExecutor, connectExecutor, sessionExecutor);
    }

    protected static class ConnectTaskExecutor implements TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

        protected final Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> processor;
        
        public ConnectTaskExecutor(
                Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> processor) {
            this.processor = processor;
        }
        
        @Override
        public ListenableFuture<ConnectMessage.Response> submit(
                Pair<ConnectMessage.Request, Publisher> input) {
            try {
                ConnectMessage.Response response = processor.apply(input.first());
                // FIXME
                /*
                if (output instanceof ConnectMessage.Response.Valid) {
                    MessagePacket message = MessagePacket.of(MessageSessionOpen.of(output.getSessionId(), output.getTimeOut()));
                    for (Identifier ensemble: peers) {
                        try {
                            ClientPeerConnection connection = peers.getConnectionForEnsemble(ensemble);
                            connection.second().write(message);
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            throw Throwables.propagate(e);
                        }
                    }
                }*/
                input.second().post(response);
                return Futures.immediateFuture(response);
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
        
    }
}
