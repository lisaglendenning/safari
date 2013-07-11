package edu.uw.zookeeper.orchestra;
import java.lang.annotation.*;

import com.google.common.util.concurrent.Service;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface DependsOn {
    Class<? extends Service>[] value() default {};
}
