package edu.uw.zookeeper.orchestra.control;

import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.ZNodeSchema.Builder.ZNodeTraversal;

public class SchemaInstance implements Reference<Schema> {
        
    public static SchemaInstance newInstance(Object root) {
        return new SchemaInstance(root);
    }
    
    private final Schema schema;
    private final Map<Object, Schema.SchemaNode> byElement;
    
    public SchemaInstance(Object root) {
        Schema schema = null;
        Iterator<ZNodeTraversal.Element> itr = 
                Schema.ZNodeSchema.Builder.traverse(root);
        ImmutableMap.Builder<Object, Schema.SchemaNode> byElement = ImmutableMap.builder();
        while (itr.hasNext()) {
            ZNodeTraversal.Element next = itr.next();
            Schema.ZNodeSchema nextSchema = next.getBuilder().build();
            ZNodeLabel.Path path =(ZNodeLabel.Path)  ZNodeLabel.Path.joined(next.getPath(), nextSchema.getLabel());
            if (schema == null) {
                if (path.isRoot()) {
                    schema = Schema.of(nextSchema);
                } else {
                    schema = Schema.of(Schema.ZNodeSchema.getDefault());
                }
            }
            Schema.SchemaNode node;
            if (path.isRoot()) {
                node = schema.root();
            } else {
                node = schema.add(path, nextSchema);
            }
            byElement.put(next.getElement(), node);
        }
        this.schema = schema;
        this.byElement = byElement.build();
    }

    @Override
    public Schema get() {
        return schema;
    }
    
    public Schema.SchemaNode byElement(Object type) {
        return byElement.get(type);
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
