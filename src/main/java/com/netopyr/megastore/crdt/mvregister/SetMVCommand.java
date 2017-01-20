package com.netopyr.megastore.crdt.mvregister;

import com.netopyr.megastore.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SetMVCommand<T> extends CrdtCommand {

    private final MVRegister.Entry<T> entry;

    public SetMVCommand(String crdtId, MVRegister.Entry<T> entry) {
        super(crdtId);
        this.entry = entry;
    }

    public MVRegister.Entry<T> getEntry() {
        return entry;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .appendSuper(super.toString())
                .append("entry", entry)
                .toString();
    }
}
