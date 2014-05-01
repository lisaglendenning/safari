package edu.uw.zookeeper.safari.cli;

import java.io.IOException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
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
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.PrefixCreator;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;
import edu.uw.zookeeper.safari.peer.protocol.ObjectMapperBuilder;

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
        ENTITY,
        @CommandDescriptor( 
                arguments = {
                        @ArgumentDescriptor(token = TokenType.ENUM, type = EntityType.class),
                        @ArgumentDescriptor(token = TokenType.STRING)})
        LOOKUP;
    }
    
    public static enum EntityType {
        VOLUME, REGION;

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

    protected final Shell shell;
    protected final ObjectMapper mapper;
    
    public ControlInvoker(
            ObjectMapper mapper,
            Shell shell) {
        this.mapper = mapper;
        this.shell = shell;
    }

    @Override
    public void invoke(Invocation<Command> input)
            throws Exception {
        Materializer<ControlZNode<?>,?> materializer = shell.getEnvironment().get(MATERIALIZER_KEY);
        // TODO: DRY
        switch (input.getCommand().second()) {
        case ENTITY:
        {
            switch ((EntityType) input.getArguments()[1]) {
            case VOLUME:
            {
                final ZNodePath path = mapper.readValue((String) input.getArguments()[2], ZNodePath.class);
                ListenableFuture<Identifier> future = ControlZNode.CreateEntity.call(
                        ControlSchema.Safari.Volumes.PATH, path, materializer);
                Futures.addCallback(future, new FutureCallback<Identifier>(){
                    @Override
                    public void onSuccess(Identifier result) {
                        try {
                            shell.println(String.format("Volume %s created => %s", path, result));
                            shell.flush();
                        } catch (IOException e) {
                        }
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            shell.printThrowable(t);
                        } catch (IOException e) {
                        }
                    }});
                break;
            }
            case REGION:
            {
                final EnsembleView<ServerInetAddressView> ensemble = mapper.readValue((String) input.getArguments()[2], new TypeReference<EnsembleView<ServerInetAddressView>>() {});
                ListenableFuture<Identifier> future = 
                        ControlZNode.CreateEntity.call(
                                ControlSchema.Safari.Regions.PATH, ensemble, materializer);
                Futures.addCallback(future, new FutureCallback<Identifier>(){
                    @Override
                    public void onSuccess(Identifier result) {
                        try {
                            shell.println(String.format("Region %s created => %s", ensemble, result));
                            shell.flush();
                        } catch (IOException e) {
                        }
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            shell.printThrowable(t);
                        } catch (IOException e) {
                        }
                    }});
                break;
            }
            }
            break;
        }
        case LOOKUP:
        {
            switch ((EntityType) input.getArguments()[1]) {
            case VOLUME:
            {
                final ZNodePath path = mapper.readValue((String) input.getArguments()[2], ZNodePath.class);
                ListenableFuture<Identifier> future = ControlZNode.LookupEntity.call(ControlSchema.Safari.Volumes.PATH, path, materializer);
                Futures.addCallback(future, new FutureCallback<Identifier>(){
                    @Override
                    public void onSuccess(Identifier result) {
                        try {
                            shell.println(String.format("Volume %s found => %s", path, result));
                            shell.flush();
                        } catch (IOException e) {
                        }
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            shell.printThrowable(t);
                        } catch (IOException e) {
                        }
                    }});
                break;
            }
            case REGION:
            {
                final EnsembleView<ServerInetAddressView> ensemble = mapper.readValue((String) input.getArguments()[2], new TypeReference<EnsembleView<ServerInetAddressView>>() {});
                ListenableFuture<Identifier> future = ControlZNode.LookupEntity.call(ControlSchema.Safari.Regions.PATH, ensemble, materializer);
                Futures.addCallback(future, new FutureCallback<Identifier>(){
                    @Override
                    public void onSuccess(Identifier result) {
                        try {
                            shell.println(String.format("Region %s found => %s", ensemble, result));
                            shell.flush();
                        } catch (IOException e) {
                        }
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            shell.printThrowable(t);
                        } catch (IOException e) {
                        }
                    }});
                break;
            }
            }
            break;
        }
        default:
            break;
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
}