package edu.uw.zookeeper.safari;

public abstract class SafariException extends Exception {

    private static final long serialVersionUID = -3981710876826606131L;
    
    protected SafariException() {
        super();
    }
    
    protected SafariException(String message) {
        super(message);
    }
    
    protected SafariException(String message, Throwable cause) {
        super(message, cause);
    }

    protected SafariException(Throwable cause) {
        super(cause);
    }
}
