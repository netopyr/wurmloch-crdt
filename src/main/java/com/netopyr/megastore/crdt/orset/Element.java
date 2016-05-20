package com.netopyr.megastore.crdt.orset;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.UUID;

final class Element<T> {

    private final T value;
    private final UUID uuid;

    Element(T value, UUID uuid) {
        this.value = value;
        this.uuid = uuid;
    }

    T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Element<?> element = (Element<?>) o;

        return new EqualsBuilder()
                .append(value, element.value)
                .append(uuid, element.uuid)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(value)
                .append(uuid)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("value", value)
                .append("uuid", uuid)
                .toString();
    }
}
