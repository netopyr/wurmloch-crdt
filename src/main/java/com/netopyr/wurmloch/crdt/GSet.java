package com.netopyr.wurmloch.crdt;

import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
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

@SuppressWarnings("WeakerAccess")
public class GSet<E> extends AbstractSet<E> implements Crdt<GSet<E>, GSet.AddCommand<E>> {

    // fields
    private final String crdtId;
    private final Set<E> elements = new HashSet<>();
    private final Processor<AddCommand<E>, AddCommand<E>> commands = ReplayProcessor.create();


    // constructor
    public GSet(String crdtId) {
        this.crdtId = Objects.requireNonNull(crdtId, "Id must not be null");
    }


    // crdt
    @Override
    public String getCrdtId() {
        return crdtId;
    }

    @Override
    public void subscribe(Subscriber<? super AddCommand<E>> subscriber) {
        commands.subscribe(subscriber);
    }

    @Override
    public void subscribeTo(Publisher<? extends AddCommand<E>> publisher) {
        Flowable.fromPublisher(publisher).onTerminateDetach().subscribe(command -> {
            if (processCommand(command)) {
                commands.onNext(command);
            }
        });
    }

    private boolean processCommand(AddCommand<E> command) {
        return doAdd(command.getElement());
    }


    // core functionality
    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public Iterator<E> iterator() {
        return new GSetIterator();
    }

    @Override
    public boolean add(E element) {
        commands.onNext(new AddCommand<>(crdtId, element));
        return doAdd(element);
    }


    // implementation
    private synchronized boolean doAdd(E element) {
        return elements.add(element);
    }

    private class GSetIterator implements Iterator<E> {

        final Iterator<E> it = elements.iterator();

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            return it.next();
        }
    }


    // commands
    public static final class AddCommand<T> extends CrdtCommand {

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
