package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.function.BiFunction;

class SimpleCrdt implements Crdt {

    private final String id;
    private final transient Subject<CrdtCommand> stream = PublishSubject.create();

    SimpleCrdt(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Observable<CrdtCommand> onCommand() {
        return stream;
    }

    @Override
    public BiFunction<Replica, String, Crdt> getFactory() {
        return (replica, id) -> new SimpleCrdt(id);
    }

    public void sendCommands(CrdtCommand... commands) {
        for (final CrdtCommand command : commands) {
            stream.onNext(command);
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
