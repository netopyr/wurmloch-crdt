package com.netopyr.megastore.crdt.orset;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

final class AddCommand<T> extends CrdtCommand {

    private final Element<T> element;

    AddCommand(String crdtId, Element<T> element) {
        super(crdtId);
        this.element = element;
    }

    Element<T> getElement() {
        return element;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AddCommand<?> that = (AddCommand<?>) o;

        return new EqualsBuilder()
                .append(getCrdtId(), that.getCrdtId())
                .append(element, that.element)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getCrdtId())
                .append(element)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("crdtId", getCrdtId())
                .append("element", element)
                .toString();
    }
}
