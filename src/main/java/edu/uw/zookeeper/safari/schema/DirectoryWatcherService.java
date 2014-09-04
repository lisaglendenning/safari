package edu.uw.zookeeper.safari.schema;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeSchema;

public class DirectoryWatcherService<T extends SafariZNode<?,?>> extends ServiceListenersService {

    public static <U extends SafariZNode<U,?>, T extends U> DirectoryWatcherService<T> listen(
            Class<?> type,
            SchemaClientService<U,?> client,
            Iterable<? extends Service.Listener> listeners) {
        DirectoryWatcherService<T> service = create(type, client, listeners);
        Watchers.watchChildren(service.schema().path(), client.materializer(), service, client.notifications());
        return service;
    }
    
    public static <U extends SafariZNode<U,?>, T extends U> DirectoryWatcherService<T> create(
            Class<?> type,
            SchemaClientService<U,?> client,
            Iterable<? extends Service.Listener> listeners) {
        DirectoryWatcherService<T> service = new DirectoryWatcherService<T>(client.materializer().schema().apply(type), listeners);
        return service;
    }
    
    protected final ValueNode<ZNodeSchema> schema;
    
    protected DirectoryWatcherService(
            ValueNode<ZNodeSchema> schema,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.schema = schema;
    }
    
    public ValueNode<ZNodeSchema> schema() {
        return schema;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(((Class<?>) schema.get().getDeclaration()).getName()).toString();
    }
}
