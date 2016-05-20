package com.netopyr.megastore.crdt.orset;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.hamcrest.CustomMatcher;

public class AddCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

    private final String crdtId;
    private final T value;

    public AddCommandMatcher(String crdtId, T value) {
        super(String.format("AddCommandMatcher[crdtId=%s,elementValue=%s]", crdtId, value));
        this.crdtId = crdtId;
        this.value = value;
    }

    @Override
    public boolean matches(Object o) {
        if (o instanceof AddCommand) {
            final AddCommand command = (AddCommand) o;
            return command.getCrdtId().equals(crdtId) && command.getElement().getValue().equals(value);
        }
        return false;
    }
}
