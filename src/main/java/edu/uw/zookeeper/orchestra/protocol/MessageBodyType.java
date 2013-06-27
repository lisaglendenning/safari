package edu.uw.zookeeper.orchestra.protocol;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MessageBodyType {
    MessageType type();
}
