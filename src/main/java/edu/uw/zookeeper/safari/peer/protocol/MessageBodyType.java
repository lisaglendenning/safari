package edu.uw.zookeeper.safari.peer.protocol;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MessageBodyType {
    MessageType value();
}
