package com.netopyr.megastore.crdt.orset;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.hamcrest.CustomMatcher;

import java.util.Arrays;
import java.util.List;

public class RemoveCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

    private final String crdtId;
    private final List<T> values;

    @SafeVarargs
    public RemoveCommandMatcher(String crdtId, T... values) {
        super(String.format("RemoveCommandMatcher[crdtId=%s,values=%s]", crdtId, Arrays.toString(values)));
        this.crdtId = crdtId;
        this.values = Arrays.asList(values);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean matches(Object o) {
        if (o instanceof RemoveCommand) {
            final RemoveCommand<T> command = (RemoveCommand<T>) o;
            if (! command.getCrdtId().equals(crdtId)) {
                return false;
            }
            for (final Element<T> element : command.getElements()) {
                if (! values.contains(element.getValue())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
