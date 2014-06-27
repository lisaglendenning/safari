package edu.uw.zookeeper.common;

import java.util.Queue;

import org.apache.logging.log4j.Logger;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;

public class SubmitActor<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends ListenableFutureActor<I, O, SubmittedRequest<I, O>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmitActor<I,O> create(
            ClientExecutor<? super I, O, ?> client,
            Queue<SubmittedRequest<I, O>> mailbox,
            Logger logger) {
        return new SubmitActor<I,O>(client, mailbox, logger);
    }
    
    protected final ClientExecutor<? super I, O, ?> client;
    
    protected SubmitActor(
            ClientExecutor<? super I, O, ?> client,
            Queue<SubmittedRequest<I, O>> mailbox,
            Logger logger) {
        super(mailbox, logger);
        this.client = client;
    }

    @Override
    public SubmittedRequest<I,O> submit(I request) {
        SubmittedRequest<I,O> future = SubmittedRequest.submit(client, request);
        if (!send(future)) {
            future.cancel(false);
        }
        return future;
    }

    @Override
    protected boolean doApply(SubmittedRequest<I, O> input) throws Exception {
        Operations.unlessError(input.get().record());
        return true;
    }
}
