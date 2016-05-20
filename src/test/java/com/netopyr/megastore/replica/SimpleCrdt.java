package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

public class SimpleCrdt implements Crdt {

    private final String id;
    private final transient Subject<CrdtCommand, CrdtCommand> stream = PublishSubject.create();

    public SimpleCrdt(Replica replica, String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Observable<CrdtCommand> onCommands() {
        return stream.asObservable();
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
