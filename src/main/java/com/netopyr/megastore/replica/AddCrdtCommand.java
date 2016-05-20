package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class AddCrdtCommand extends CrdtCommand {

    private final Class<? extends Crdt> crdtClass;

    public AddCrdtCommand(String crdtId, Class<? extends Crdt> crdtClass) {
        super(crdtId);
        this.crdtClass = crdtClass;
    }

    public Class<? extends Crdt> getCrdtClass() {
        return crdtClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AddCrdtCommand that = (AddCrdtCommand) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(getCrdtId(), that.getCrdtId())
                .append(crdtClass, that.crdtClass)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(getCrdtId())
                .append(crdtClass)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("crdtId", getCrdtId())
                .append("crdtClass", crdtClass)
                .toString();
    }
}
