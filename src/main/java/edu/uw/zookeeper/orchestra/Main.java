package edu.uw.zookeeper.orchestra;


import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.common.Configuration;

public class Main extends DefaultMain {

    public static void main(String[] args) {
        main(args, ConfigurableApplicationFactory.newInstance(Main.class));
    }

    public Main(Configuration configuration) {
        super(MainApplicationModule.main(), configuration);
    }
}
