package com.netopyr.wurmloch.store;

import com.netopyr.wurmloch.crdt.AbstractCrdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import io.reactivex.processors.ReplayProcessor;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

class SimpleCrdt extends AbstractCrdt<SimpleCrdt, CrdtCommand> {

    // constructor
    SimpleCrdt(String nodeId, String crdtId) {
        super(nodeId, crdtId, ReplayProcessor.create());
    }


    // crdt
    @Override
    protected boolean processCommand(CrdtCommand command) {
        return ! Objects.equals(((SimpleCommand) command).getNodeId(), nodeId);
    }


    // functionality
    void sendCommands(SimpleCommand... commands) {
        for (final SimpleCommand command : commands) {
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


    public static final class SimpleCommand extends CrdtCommand {

        private final String nodeId;

        public SimpleCommand(String nodeId, String crdtId) {
            super(crdtId);
            this.nodeId = nodeId;
        }

        public String getNodeId() {
            return nodeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            SimpleCommand that = (SimpleCommand) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(nodeId, that.nodeId)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(nodeId)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("nodeId", nodeId)
                    .toString();
        }
    }
}
