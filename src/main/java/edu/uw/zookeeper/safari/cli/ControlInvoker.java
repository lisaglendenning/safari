package edu.uw.zookeeper.safari.cli;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.client.cli.ArgumentDescriptor;
import edu.uw.zookeeper.client.cli.ClientExecutorInvoker;
import edu.uw.zookeeper.client.cli.CommandDescriptor;
import edu.uw.zookeeper.client.cli.Environment;
import edu.uw.zookeeper.client.cli.Invocation;
import edu.uw.zookeeper.client.cli.Invoker;
import edu.uw.zookeeper.client.cli.Invokes;
import edu.uw.zookeeper.client.cli.Shell;
import edu.uw.zookeeper.client.cli.TokenType;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.LookupEntity;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.PrepareVolumeOperation;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationCoordinatorEntry;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;
import edu.uw.zookeeper.safari.peer.protocol.ObjectMapperBuilder;
import edu.uw.zookeeper.safari.schema.PrefixCreator;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;

public class ControlInvoker extends AbstractIdleService implements Invoker<ControlInvoker.Command> {

    @Invokes(commands={Command.class})
    public static ControlInvoker create(Shell shell) {
        ObjectMapper mapper = ObjectMapperBuilder.defaults().build();
        return new ControlInvoker(mapper, shell);
    }

    public static enum Command {
        @CommandDescriptor( 
                arguments = {
                        @ArgumentDescriptor(token = TokenType.ENUM, type = EntityType.class),
                        @ArgumentDescriptor(token = TokenType.STRING)})
        LOOKUP,
        
        @CommandDescriptor(arguments = {
                @ArgumentDescriptor(name = "volume", token = TokenType.STRING),
                @ArgumentDescriptor(name = "region", token = TokenType.STRING) })
        TRANSFER,
        
        @CommandDescriptor(arguments = {
                @ArgumentDescriptor(name = "volume", token = TokenType.STRING) })
        MERGE,
        
        @CommandDescriptor(arguments = {
                @ArgumentDescriptor(name = "volume", token = TokenType.STRING),
                @ArgumentDescriptor(name = "region", token = TokenType.STRING),
                @ArgumentDescriptor(name = "branch", token = TokenType.STRING) })
        SPLIT;
    }
    
    public static enum EntityType {
        PEER, VOLUME, REGION;

        public static EntityType fromString(String value) {
            for (EntityType e : values()) {
                if (e.toString().equals(value)) {
                    return e;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }
    
    public static final Environment.Key<Materializer<ControlZNode<?>,?>> MATERIALIZER_KEY = Environment.Key.create("MATERIALIZER", Materializer.class);

    protected final Logger logger;
    protected final Shell shell;
    protected final ObjectMapper mapper;
    
    public ControlInvoker(
            ObjectMapper mapper,
            Shell shell) {
        this.logger = LogManager.getLogger(this);
        this.mapper = mapper;
        this.shell = shell;
    }
    
    public Materializer<ControlZNode<?>,?> getMaterializer() {
        return shell.getEnvironment().get(MATERIALIZER_KEY);
    }

    @Override
    public void invoke(Invocation<Command> input)
            throws Exception {
        switch (input.getCommand().second()) {
        case LOOKUP:
        {
            switch ((EntityType) input.getArguments()[1]) {
            case PEER:
            {
                final ServerInetAddressView address = mapper.readValue((String) input.getArguments()[2], ServerInetAddressView.class);
                lookup(String.format("Peer %s found => %%s", address), ControlSchema.Safari.Peers.class, address);
                break;
            }
            case VOLUME:
            {
                final ZNodePath path = mapper.readValue((String) input.getArguments()[2], ZNodePath.class);
                lookup(String.format("Volume %s found => %%s", path), ControlSchema.Safari.Volumes.class, path);
                break;
            }
            case REGION:
            {
                final EnsembleView<ServerInetAddressView> ensemble = mapper.readValue((String) input.getArguments()[2], new TypeReference<EnsembleView<ServerInetAddressView>>() {});
                lookup(String.format("Region %s found => %%s", ensemble), ControlSchema.Safari.Regions.class, ensemble);
                break;
            }
            }
            break;
        }
        default:
        {
            Identifier volume = Identifier.valueOf((String) input.getArguments()[1]);
            VolumeOperator operator;
            ImmutableList<?> arguments;
            switch (input.getCommand().second()) {
            case MERGE:
                operator = VolumeOperator.MERGE;
                arguments = ImmutableList.of();
                break;
            case SPLIT:
                operator = VolumeOperator.SPLIT;
                arguments = ImmutableList.of(
                        Identifier.valueOf((String) input.getArguments()[2]),
                        ZNodeName.fromString((String) input.getArguments()[3]));
                break;
            case TRANSFER:
                operator = VolumeOperator.TRANSFER;
                arguments = ImmutableList.of(
                        Identifier.valueOf((String) input.getArguments()[2]));
                break;
            default:
                throw new AssertionError();
            }
            propose(volume, operator, arguments);
            break;
        }
        }
    }

    @Override
    protected void startUp() throws Exception {
        for (Command command: Command.values()) {
            shell.getCommands().withCommand(command);
        }

        Materializer<ControlZNode<?>,?> materializer = 
                Materializer.<ControlZNode<?>, Message.ServerResponse<?>>fromHierarchy(
                        ControlSchema.class,
                        JacksonSerializer.create(mapper),
                        shell.getEnvironment().get(ClientExecutorInvoker.CLIENT_KEY).getConnectionClientExecutor());
        shell.getEnvironment().put(MATERIALIZER_KEY, materializer);

        Futures.allAsList(PrefixCreator.forMaterializer(materializer).call()).get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    protected <V,T extends ControlZNode<V>,U extends ControlZNode.IdentifierControlZNode,C extends ControlZNode.EntityDirectoryZNode<V,T,U>> PrintCallback<Optional<Identifier>> lookup(
            final String format, Class<C> type, V value) {
        return new PrintCallback<Optional<Identifier>>(
                new Function<Optional<Identifier>,String>() {
                    @Override
                    public String apply(Optional<Identifier> input) {
                        return String.format(format, input.orNull());
                    }
                },
                LookupEntity.sync(
                        value, 
                        type, 
                        getMaterializer()));
    }
    
    protected PrintCallback<VolumeLogEntryPath> propose(
            final Identifier volume, final VolumeOperator operator, final List<?> arguments) {
        final ListenableFuture<VolumeOperation<?>> operation =
                PrepareVolumeOperation.create(
                        VolumesSchemaRequests.create(getMaterializer()).volume(volume), 
                        operator, arguments,
                        SettableFuturePromise.<VolumeOperation<?>>create());
        LoggingFutureListener.listen(logger, operation);
        return new PrintCallback<VolumeLogEntryPath>(
                new Function<VolumeLogEntryPath, String>() {
                    @Override
                    public String apply(VolumeLogEntryPath input) {
                        try {
                            return String.format("Proposed %s => %s", operation.get(), input);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            throw new AssertionError(e);
                        }
                    }
                },
                Futures.transform(
                        operation, 
                        new AsyncFunction<VolumeOperation<?>,VolumeLogEntryPath>() {
                            @Override
                            public VolumeOperationCoordinatorEntry apply(
                                    VolumeOperation<?> input) throws Exception {
                                return VolumeOperationCoordinatorEntry.newEntry(input, getMaterializer());
                            }
                }));
    }
    
    protected class PrintCallback<V> extends ForwardingListenableFuture<V> implements Runnable {

        protected final Function<V, String> format;
        protected final ListenableFuture<V> future;
        
        public PrintCallback(
                Function<V, String> format,
                ListenableFuture<V> future) {
            this.format = format;
            this.future = future;
            addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void run() {
            if (isDone()) {
                try {
                    try {
                        shell.println(format.apply(get()));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        shell.printThrowable(e);
                    }
                    shell.flush();
                } catch (IOException e) {
                }
            }
        }

        @Override
        protected ListenableFuture<V> delegate() {
            return future;
        }
    }
}