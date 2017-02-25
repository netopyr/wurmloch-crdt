package com.netopyr.wurmloch.store;

import com.netopyr.wurmloch.crdt.AbstractCrdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import io.reactivex.processors.PublishProcessor;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.function.BiFunction;

class SimpleCrdt extends AbstractCrdt<SimpleCrdt, CrdtCommand> {

    // constructor
    SimpleCrdt(String crdtId) {
        super("", crdtId, PublishProcessor.create());
    }

    @Override
    public BiFunction<String, String, SimpleCrdt> getFactory() {
        return (nodeId, crtdId) -> new SimpleCrdt(crtdId);
    }


    // crdt
    @Override
    protected boolean processCommand(CrdtCommand command) {
        return false;
    }


    // functionality
    void sendCommands(CrdtCommand... commands) {
        for (final CrdtCommand command : commands) {
            this.commands.onNext(command);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SimpleCrdt that = (SimpleCrdt) o;

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
        return new ToStringBuilder(this)
                .append("crdtId", crdtId)
                .toString();
    }
}
