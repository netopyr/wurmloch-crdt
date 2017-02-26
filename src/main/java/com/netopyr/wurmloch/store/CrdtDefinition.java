package com.netopyr.wurmloch.store;

import com.netopyr.wurmloch.crdt.Crdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Publisher;

public class CrdtDefinition {

    private final String crdtId;
    private final Class<? extends Crdt> crdtClass;
    private final Publisher<? extends CrdtCommand> publisher;

    @SuppressWarnings("unchecked")
    CrdtDefinition(String crdtId, Class<? extends Crdt> crdtClass, Publisher<? extends CrdtCommand> publisher) {
        this.crdtId = crdtId;
        this.crdtClass = crdtClass;
        this.publisher = publisher;
    }

    public String getCrdtId() {
        return crdtId;
    }

    public Class<? extends Crdt> getCrdtClass() {
        return crdtClass;
    }

    public Publisher<? extends CrdtCommand> getPublisher() {
        return publisher;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("crdtId", crdtId)
                .append("crdtClass", crdtClass)
                .toString();
    }
}
