package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.vectorclock.VectorClock;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SetLWWCommand<T> extends CrdtCommand {

    private final String nodeId;
    private final T value;
    private final VectorClock clock;

    public SetLWWCommand(String crdtId, String nodeId, T value, VectorClock clock) {
        super(crdtId);
        this.nodeId = nodeId;
        this.value = value;
        this.clock = clock;
    }

    public String getNodeId() {
        return nodeId;
    }

    public T getValue() {
        return value;
    }

    public VectorClock getClock() {
        return clock;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("nodeId", nodeId)
                .append("value", value)
                .append("clock", clock)
                .toString();
    }
}
