package edu.uw.zookeeper.orchestra.backend;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.Label;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.control.SchemaInstance;

@ZNode(acl=Acls.Definition.ANYONE_ALL, label="/")
public abstract class BackendSchema {

    @ZNode
    public static abstract class Volumes {

        @Label
        public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("volumes");

        @ZNode
        public static class Root {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;
        }
    }

    public static SchemaInstance getInstance() {
        return Holder.getInstance().get();
    }
    
    public static enum Holder implements Reference<SchemaInstance> {
        SCHEMA(BackendSchema.class);
        
        public static Holder getInstance() {
            return SCHEMA;
        }
        
        private final SchemaInstance instance;
        
        private Holder(Object root) {
            this.instance = SchemaInstance.newInstance(root);
        }
    
        @Override
        public SchemaInstance get() {
            return instance;
        }
    }
}
