package com.netopyr.megastore.crdt;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public abstract class CrdtCommand {

    private final String crdtId;

    public CrdtCommand(String crdtId) {
        this.crdtId = crdtId;
    }

    public String getCrdtId() {
        return crdtId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        CrdtCommand that = (CrdtCommand) o;

        return new EqualsBuilder()
                .append(crdtId, that.crdtId)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(crdtId)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("crdtId", crdtId)
                .toString();
    }
}
