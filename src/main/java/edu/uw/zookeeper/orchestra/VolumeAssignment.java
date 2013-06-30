package edu.uw.zookeeper.orchestra;

import com.google.common.base.Objects;

public class VolumeAssignment {

    public static VolumeAssignment of(
            Identifier id,
            VolumeDescriptor descriptor, 
            Identifier assignment) {
        return new VolumeAssignment(id, descriptor, assignment);
    }
    
    protected final Identifier id;
    protected final VolumeDescriptor descriptor;
    protected final Identifier assignment;
    
    public VolumeAssignment(
            Identifier id,
            VolumeDescriptor descriptor, 
            Identifier assignment) {
        this.id = id;
        this.descriptor = descriptor;
        this.assignment = assignment;
    }
    
    public Identifier getId() {
        return id;
    }

    public VolumeAssignment setId(Identifier id) {
        if (id.equals(getId())) {
            return this;
        } else {
            return of(id, getDescriptor(), getAssignment());
        }
    }

    public VolumeDescriptor getDescriptor() {
        return descriptor;
    }

    public VolumeAssignment setDescriptor(VolumeDescriptor descriptor) {
        if (descriptor.equals(getDescriptor())) {
            return this;
        } else {
            return of(getId(), descriptor, getAssignment());
        }
    }

    public Identifier getAssignment() {
        return assignment;
    }
    
    public VolumeAssignment setAssignment(Identifier assignment) {
        if (assignment.equals(getAssignment())) {
            return this;
        } else {
            return of(getId(), getDescriptor(), assignment);
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", getId())
                .add("descriptor", getDescriptor())
                .add("assignment", getAssignment())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId(), getDescriptor(), getAssignment());
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
        return Objects.equal(getId(), other.getId())
                && Objects.equal(getDescriptor(), other.getDescriptor())
                && Objects.equal(getAssignment(), other.getAssignment());
    }
}
