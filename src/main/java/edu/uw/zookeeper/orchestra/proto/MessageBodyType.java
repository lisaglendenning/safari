package edu.uw.zookeeper.orchestra.proto;

import java.lang.annotation.*;

@Documented
@Inherited
public @interface MessageBodyType {
    MessageType type();
}
