package edu.uw.zookeeper.orchestra.frontend;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpen;
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
            ServiceLocator locator,
            SessionTable sessions,
            Generator<Long> zxids) {
        TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                ServerTaskExecutor.ProcessorExecutor.of(
                        FourLetterRequestProcessor.getInstance());
        ConnectTaskExecutor connectExecutor = new ConnectTaskExecutor(
                locator,
                ConnectTableProcessor.create(sessions, zxids));
        TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> sessionExecutor = null;
        return new FrontendServerTaskExecutor(
                locator,
                anonymousExecutor,
                connectExecutor,
                sessionExecutor);
    }
    
    protected final ServiceLocator locator;
    
    protected FrontendServerTaskExecutor(
            ServiceLocator locator,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        super(anonymousExecutor, connectExecutor, sessionExecutor);
        this.locator = locator;
    }

    protected static class ConnectTaskExecutor implements TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

        protected final ServiceLocator locator;
        
        protected final Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> processor;
        
        public ConnectTaskExecutor(
                ServiceLocator locator,
                Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> processor) {
            this.processor = processor;
            this.locator = locator;
        }
        
        @Override
        public ListenableFuture<ConnectMessage.Response> submit(
                Pair<ConnectMessage.Request, Publisher> input) {
            try {
                ConnectMessage.Response output = processor.apply(input.first());
                if (output instanceof ConnectMessage.Response.Valid) {
                    // TODO
                    MessagePacket message = MessagePacket.of(MessageSessionOpen.of(output.getSessionId(), output.getTimeOut()));
                    locator.getInstance(EnsembleConnectionsService.class).broadcast(message);
                }
                input.second().post(output);
                return Futures.immediateFuture(output);
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
        
    }
}
