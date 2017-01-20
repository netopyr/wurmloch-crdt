package com.netopyr.megastore.crdt.mvregister;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.hamcrest.CustomMatcher;

import java.util.Objects;

public class SetMVCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

    private final String crdtId;
    private final T value;

    public SetMVCommandMatcher(String crdtId, T value) {
        super(String.format("SetMVCommandMatcher[crdtId=%s,value=%s]", crdtId, value));
        this.crdtId = crdtId;
        this.value = value;
    }

    @Override
    public boolean matches(Object o) {
        if (o instanceof SetMVCommand) {
            final SetMVCommand command = (SetMVCommand) o;
            return Objects.equals(command.getCrdtId(), crdtId)
                    && Objects.equals(command.getEntry().getValue(), value)
                    && command.getEntry().getClock() != null;
        }
        return false;
    }
}
