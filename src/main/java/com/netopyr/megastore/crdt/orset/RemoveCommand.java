package com.netopyr.megastore.crdt.orset;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Set;

final class RemoveCommand<T> extends CrdtCommand {

    private final Set<Element<T>> elements;

    RemoveCommand(String crdt, Set<Element<T>> elements) {
        super(crdt);
        this.elements = elements;
    }

    Set<Element<T>> getElements() {
        return elements;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        RemoveCommand<?> that = (RemoveCommand<?>) o;

        return new EqualsBuilder()
                .append(getCrdtId(), that.getCrdtId())
                .append(elements, that.elements)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getCrdtId())
                .append(elements)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("crdtId", getCrdtId())
                .append("elements", elements)
                .toString();
    }
}
