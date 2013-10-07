package edu.uw.zookeeper.safari.backend;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.Label;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.SchemaInstance;

// TODO: reuse functionality in Control
@ZNode(acl=Acls.Definition.ANYONE_ALL)
public abstract class BackendSchema {

    @Label
    public static final ZNodeLabel.Path ROOT = ControlSchema.ROOT;
    
    @ZNode
    public static abstract class Volumes {

        @Label
        public static ZNodeLabel.Component LABEL = ZNodeLabel.Component.of("volumes");

        public static ZNodeLabel.Path path() {
            return BackendSchema.getInstance().byElement(BackendSchema.Volumes.class).path();
        }

        public static ZNodeLabel suffix(ZNodeLabel.Path path) {
            return path.suffix(path().length());
        }
        
        @ZNode
        public static class Root {

            @Label(type=LabelType.PATTERN)
            public static final String LABEL_PATTERN = Identifier.PATTERN;
            
            public static Identifier getShard(ZNodeLabel.Path path) {
                ZNodeLabel label = Volumes.suffix(path);
                if (label instanceof ZNodeLabel.Path) {
                    label = ((ZNodeLabel.Path) label).prefix(label.toString().indexOf(ZNodeLabel.SLASH));
                }
                if (label instanceof ZNodeLabel.Component) {
                    return Identifier.valueOf(label.toString());
                }
                return null;
            }
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
