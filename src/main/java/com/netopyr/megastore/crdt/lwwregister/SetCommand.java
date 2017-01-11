package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.vectorclock.VectorClock;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SetCommand<T> extends CrdtCommand {

    private final String replicaId;
    private final T value;
    private final VectorClock clock;

    public SetCommand(String crdtId, String replicaId, T value, VectorClock clock) {
        super(crdtId);
        this.replicaId = replicaId;
        this.value = value;
        this.clock = clock;
    }

    public String getReplicaId() {
        return replicaId;
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
                .append("replicaId", replicaId)
                .append("value", value)
                .append("clock", clock)
                .toString();
    }
}
