package edu.uw.zookeeper.safari.storage.schema;

import com.google.common.base.Converter;

/**
 * TODO: ambiguous if backslash is adjacent to a slash
 */
public final class EscapedConverter extends Converter<String, String> {

    public static EscapedConverter getInstance() {
        return Instance.get();
    }
    
    private static enum Instance {
        INSTANCE;
        
        public static EscapedConverter get() {
            return INSTANCE.instance;
        }
        
        private final EscapedConverter instance = new EscapedConverter();
    }
    
    protected EscapedConverter() {}
    
    @Override
    protected String doForward(String input) {
        StringBuilder sb = new StringBuilder(input.length()+1);
        for (int i=0; i<input.length(); ++i) {
            char c = input.charAt(i);
            switch (c) {
            case '/':
                sb.append('\\');
                break;
            case '\\':
                sb.append('\\').append('\\');
                break;
            default:
                sb.append(c);
                break;
            }
        }
        if (sb.length() == 0) {
            // special case to avoid empty label
            sb.append('\\');
        }
        return sb.toString();
    }

    @Override
    protected String doBackward(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        for (int i=0; i<input.length(); ++i) {
            char c = input.charAt(i);
            switch (c) {
            case '\\':
                if (i+1 < input.length()) {
                    if (input.charAt(i+1) == '\\') {
                        sb.append('\\');
                        ++i;
                    } else {
                        sb.append('/');
                    }
                } else {
                    // eat ending escape
                }
                break;
            default:
                sb.append(c);
                break;
            }
        }
        return sb.toString();
    }
}