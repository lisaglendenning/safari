package edu.uw.zookeeper.orchestra.proto;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MessageBodyType {
    MessageType type();
}
