package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.function.BiFunction;

public final class AddCrdtCommand extends CrdtCommand {

    private final Class<? extends Crdt> crdtClass;
    private final BiFunction<Replica, String, ? extends Crdt> factory;

    public AddCrdtCommand(Crdt crdt) {
        super(crdt.getId());
        this.crdtClass = crdt.getClass();
        this.factory = crdt.getFactory();
    }

    public BiFunction<Replica, String, ? extends Crdt> getFactory() {
        return factory;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AddCrdtCommand that = (AddCrdtCommand) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(crdtClass, that.crdtClass)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(crdtClass)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("crdtClass", crdtClass)
                .toString();
    }
}
