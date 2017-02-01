package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.PublishProcessor;
import javaslang.Function4;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class GSet<T> extends AbstractSet<T> implements Crdt /*, ObservableSet<T> */ {

    private final String id;
    private final Set<T> elements = new HashSet<>();
    private final Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();


    public GSet(String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        this.id = Objects.requireNonNull(id, "Id must not be null");
        inCommands.subscribe(new CrdtSubscriber(this::processCommand));
        commands.subscribe(outCommands);
    }


    @Override
    public String getId() {
        return id;
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return (nodeId, id, inCommands, outCommands) -> new GSet<>(id, inCommands, outCommands);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public Iterator<T> iterator() {
        return new GSetIterator();
    }

    @Override
    public boolean add(T element) {
        commands.onNext(new AddCommand<>(id, element));
        return doAdd(element);
    }

    private synchronized boolean doAdd(T element) {
        return elements.add(element);
    }

    @SuppressWarnings("unchecked")
    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (AddCommand.class.equals(clazz)) {
            doAdd(((AddCommand<T>)command).getElement());
        }
    }

    private class GSetIterator implements Iterator<T> {

        final Iterator<T> it = elements.iterator();

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            return it.next();
        }
    }


    static final class AddCommand<T> extends CrdtCommand {

        private final T element;

        AddCommand(String crdtId, T element) {
            super(crdtId);
            this.element = element;
        }

        T getElement() {
            return element;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            AddCommand<?> that = (AddCommand<?>) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(element, that.element)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(element)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("crdtId", getCrdtId())
                    .append("element", element)
                    .toString();
        }
    }
}
