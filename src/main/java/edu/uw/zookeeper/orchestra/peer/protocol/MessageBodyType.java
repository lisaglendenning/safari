package edu.uw.zookeeper.orchestra.peer.protocol;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MessageBodyType {
    MessageType type();
}
