package edu.uw.zookeeper.orchestra;

import com.google.common.base.Objects;

public class VolumeAssignment {

    public static VolumeAssignment of(
            Volume volume, 
            Identifier assignment) {
        return new VolumeAssignment(volume, assignment);
    }
    
    protected final Volume volume;
    protected final Identifier assignment;
    
    public VolumeAssignment(
            Volume volume, 
            Identifier assignment) {
        this.volume = volume;
        this.assignment = assignment;
    }
    
    public Volume getVolume() {
        return volume;
    }

    public VolumeAssignment setVolume(Volume volume) {
        if (volume.equals(getVolume())) {
            return this;
        } else {
            return of(volume, getAssignment());
        }
    }

    public Identifier getAssignment() {
        return assignment;
    }
    
    public VolumeAssignment setAssignment(Identifier assignment) {
        if (assignment.equals(getAssignment())) {
            return this;
        } else {
            return of(getVolume(), assignment);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("volume", getVolume())
                .add("assignment", getAssignment())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getVolume(), getAssignment());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        VolumeAssignment other = (VolumeAssignment) obj;
        return Objects.equal(getVolume(), other.getVolume())
                && Objects.equal(getAssignment(), other.getAssignment());
    }
}
