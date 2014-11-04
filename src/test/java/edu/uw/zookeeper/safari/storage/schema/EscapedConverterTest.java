package edu.uw.zookeeper.safari.storage.schema;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EscapedConverterTest {
    
    protected final char ESCAPE_CHAR = EscapedConverter.escapeChar();
    
    @Test
    public void testEmpty() {
        both("", String.valueOf(ESCAPE_CHAR));
    }
    
    @Test
    public void testEscape() {
        String slash = String.valueOf('/');
        String escape = String.valueOf(ESCAPE_CHAR);
        String other = "a";
        both(escape, escape+escape);  
        both(slash + other + escape + other, escape + other + escape + escape + other);  
    }
    
    @Test
    public void testSlash() {
        String slash = String.valueOf('/');
        String escape = String.valueOf(ESCAPE_CHAR);
        String other = "a";
        both(slash + other + slash + other + slash, escape + other + escape + other + escape);  
    }
    
    protected void forward(String input, String expected) {
        assertEquals(expected, EscapedConverter.getInstance().convert(input));
    }
    
    protected void reverse(String input, String expected) {
        assertEquals(expected, EscapedConverter.getInstance().reverse().convert(input));
    }
    
    protected void both(String input, String expected) {
        forward(input, expected);
        reverse(expected, input);
    }
}
