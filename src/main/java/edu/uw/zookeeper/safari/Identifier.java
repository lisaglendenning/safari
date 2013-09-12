package edu.uw.zookeeper.safari;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.base.Objects;
import com.google.common.collect.ForwardingNavigableSet;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedInteger;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.Serializes;

public class Identifier implements Comparable<Identifier> {

    public static Identifier zero() {
        return Holder.ZERO.get();
    }
    
    @Serializes(from=String.class, to=Identifier.class)
    public static Identifier valueOf(String string) {
        UnsignedInteger value = UnsignedInteger.valueOf(string, RADIX);
        return new Identifier(value);
    }

    public static Identifier valueOf(byte[] bytes) {
        return valueOf(Ints.fromByteArray(bytes));
    }

    public static Identifier valueOf(int bits) {
        return new Identifier(UnsignedInteger.fromIntBits(bits));
    }
    
    public static String toString(UnsignedInteger value) {
        // String.format won't left pad zeros for a string
        String str = value.toString(RADIX);
        StringBuilder sb = new StringBuilder();
        for (int toPrepend=CHARACTERS-str.length(); toPrepend>0; toPrepend--) {
            sb.append('0');
        }
        sb.append(str);
        return sb.toString();
    }

    public static final int BYTES = Ints.BYTES;
    public static final int RADIX = 16;
    public static final int CHARACTERS = BYTES * 2;
    public static final String PATTERN = "[a-f0-9]{" + CHARACTERS + "}";
    public static final String FORMAT = "%0" + CHARACTERS + "s";
    
    protected static enum Holder implements Reference<Identifier> {
        ZERO(new Identifier(UnsignedInteger.fromIntBits(0)));

        private final Identifier instance;
        
        private Holder(Identifier instance) {
            this.instance = instance;
        }
        
        @Override
        public Identifier get() {
            return instance;
        }
    }
    
    protected final UnsignedInteger value;
    protected final String asString;
    
    public Identifier(UnsignedInteger value) {
        this.value = checkNotNull(value);
        this.asString = toString(value);
    }
    
    public UnsignedInteger intValue() {
        return value;
    }

    @Override
    public int compareTo(Identifier o) {
        return intValue().compareTo(o.intValue());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof Identifier)) {
            return false;
        }
        Identifier other = (Identifier) obj;
        return Objects.equal(intValue(), other.intValue());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(intValue());
    }
    
    @Serializes(from=Identifier.class, to=String.class)
    @Override
    public String toString() {
        return asString;
    }
    
    public static class Space extends ForwardingNavigableSet<Identifier> {
        
        public static Space newInstance() {
            return new Space(new TreeSet<Identifier>());
        }
        
        protected final NavigableSet<Identifier> identifiers;
        
        public Space(NavigableSet<Identifier> identifiers) {
            this.identifiers = identifiers;
        }
        
        protected NavigableSet<Identifier> delegate() {
            return identifiers;
        }
        
        @Override
        public Identifier ceiling(Identifier e) {
            Identifier i = super.ceiling(e);
            if (i == null) {
                Iterator<Identifier> itr = iterator();
                if (itr.hasNext()) {
                    i = itr.next();
                }
            }
            return i;
        }
        
        @Override
        public Identifier floor(Identifier e) {
            Identifier i = super.floor(e);
            if (i == null) {
                Iterator<Identifier> itr = descendingIterator();
                if (itr.hasNext()) {
                    i = itr.next();
                }
            }
            return i;
        }
        
        @Override
        public Identifier higher(Identifier e) {
            Identifier i = super.higher(e);
            if (i == null) {
                Iterator<Identifier> itr = iterator();
                if (itr.hasNext()) {
                    i = itr.next();
                }
            }
            return i;
        }
        
        @Override
        public Identifier lower(Identifier e) {
            Identifier i = super.lower(e);
            if (i == null) {
                Iterator<Identifier> itr = descendingIterator();
                if (itr.hasNext()) {
                    i = itr.next();
                }
            }
            return i;
        }
    }
}
