package edu.uw.zookeeper.orchestra.common;
import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface DependsOn {
    Class<?>[] value() default {};
}
