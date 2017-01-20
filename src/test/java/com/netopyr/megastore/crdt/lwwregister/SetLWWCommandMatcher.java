package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.hamcrest.CustomMatcher;

import java.util.Objects;

public class SetLWWCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

    private final String nodeId;
    private final String crdtId;
    private final T value;

    public SetLWWCommandMatcher(String nodeId, String crdtId, T value) {
        super(String.format("SetLWWCommandMatcher[nodeId=%s,crdtId=%s,value=%s]", nodeId, crdtId, value));
        this.nodeId = nodeId;
        this.crdtId = crdtId;
        this.value = value;
    }

    @Override
    public boolean matches(Object o) {
        if (o instanceof SetLWWCommand) {
            final SetLWWCommand command = (SetLWWCommand) o;
            return Objects.equals(command.getNodeId(), nodeId)
                    && Objects.equals(command.getCrdtId(), crdtId)
                    && Objects.equals(command.getValue(), value)
                    && command.getClock() != null;
        }
        return false;
    }
}
