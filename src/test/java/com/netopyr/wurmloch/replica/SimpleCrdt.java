package com.netopyr.wurmloch.replica;

import com.netopyr.wurmloch.crdt.Crdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import io.reactivex.processors.PublishProcessor;
import javaslang.Function4;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

class SimpleCrdt implements Crdt {

    private final String id;
    private final transient Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();

    SimpleCrdt(String id, Subscriber<? super CrdtCommand> outCommands) {
        this.id = id;
        commands.subscribe(outCommands);
    }

    @Override
    public String getId() {
        return id;
    }


    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return (replica, id, inCommands, outCommands) -> new SimpleCrdt(id, outCommands);
    }

    public void sendCommands(CrdtCommand... commands) {
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
                .append(id, that.id)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .toString();
    }
}
