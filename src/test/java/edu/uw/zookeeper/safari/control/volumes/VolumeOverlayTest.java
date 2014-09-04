package edu.uw.zookeeper.safari.control.volumes;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Maps;

import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.volumes.VolumeOverlay;

@RunWith(JUnit4.class)
public class VolumeOverlayTest {
    
    @Test
    public void test() throws Exception {
        VolumeOverlay overlay = VolumeOverlay.empty();
        
        Map<ZNodePath, StampedValue<Identifier>> volumes = Maps.newHashMap();
        ZNodePath path = ZNodePath.root();
        long stamp = 1L;
        add(overlay, volumes, path, stamp);
        
        for (int i=0; i<2; ++i) {
            path = path.join(ZNodeLabel.fromString("1"));
            stamp++;
            add(overlay, volumes, path, stamp);
        }
        
        path = ZNodePath.root();
        for (int i=0; i<2; ++i) {
            path = path.join(ZNodeLabel.fromString("2"));
        }
        stamp++;
        add(overlay, volumes, path, stamp);

        path = ZNodePath.root();
        path = path.join(ZNodeLabel.fromString("1"));
        remove(overlay, volumes, path);
    }
    
    protected StampedValue<Identifier> add(final VolumeOverlay overlay, final Map<ZNodePath, StampedValue<Identifier>> volumes, final ZNodePath path, final long stamp) {
        Map.Entry<ZNodePath, StampedValue<Identifier>> enclosing = null;
        for (Map.Entry<ZNodePath, StampedValue<Identifier>> v: volumes.entrySet()) {
            if (path.startsWith(v.getKey()) && ((enclosing == null) || (enclosing.getKey().length() < v.getKey().length()))) {
                enclosing = v;
            }
        }
        if (enclosing != null) {
            assertEquals(enclosing.getValue(), overlay.apply(path));
        } else {
            assertNull(overlay.apply(path));
        }
        Identifier id = Hash.default32().apply(path.toString()).asIdentifier();
        StampedValue<Identifier> value = StampedValue.valueOf(stamp, id);
        volumes.put(path, value);
        overlay.add(stamp, path, id);
        for (Map.Entry<ZNodePath, StampedValue<Identifier>> entry: volumes.entrySet()) {
            assertEquals(entry.getValue(), overlay.apply(entry.getKey()));
        }
        return value;
    }
    
    protected void remove(final VolumeOverlay overlay, final Map<ZNodePath, StampedValue<Identifier>> volumes, final ZNodePath path) {
        Identifier id = volumes.remove(path).get();
        overlay.remove(path, id);
        for (Map.Entry<ZNodePath, StampedValue<Identifier>> entry: volumes.entrySet()) {
            assertEquals(entry.getValue(), overlay.apply(entry.getKey()));
        }
        Map.Entry<ZNodePath, StampedValue<Identifier>> enclosing = null;
        for (Map.Entry<ZNodePath, StampedValue<Identifier>> v: volumes.entrySet()) {
            if (path.startsWith(v.getKey()) && ((enclosing == null) || (enclosing.getKey().length() < v.getKey().length()))) {
                enclosing = v;
            }
        }
        if (enclosing != null) {
            assertEquals(enclosing.getValue(), overlay.apply(path));
        }
    }
}
