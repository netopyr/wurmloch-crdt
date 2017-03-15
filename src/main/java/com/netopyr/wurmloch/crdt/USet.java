package com.netopyr.wurmloch.crdt;

import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
import javaslang.control.Option;
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
public class USet<E> extends AbstractSet<E> implements Crdt<USet<E>, USet.USetCommand<E>> {

    // fields
    private final String crdtId;
    private final Set<E> elements = new HashSet<>();
    private final Processor<USetCommand<E>, USetCommand<E>> commands = ReplayProcessor.create();


    // constructor
    public USet(String crdtId) {
        this.crdtId = Objects.requireNonNull(crdtId, "CrdtId must not be null");
    }


    // crdt
    @Override
    public String getCrdtId() {
        return crdtId;
    }

    @Override
    public void subscribe(Subscriber<? super USetCommand<E>> subscriber) {
        commands.subscribe(subscriber);
    }

    @Override
    public void subscribeTo(Publisher<? extends USetCommand<E>> publisher) {
        Flowable.fromPublisher(publisher).onTerminateDetach().subscribe(command -> {
            final Option<USetCommand<E>> newCommand = processCommand(command);
            newCommand.peek(commands::onNext);

        });
    }

    private Option<USetCommand<E>> processCommand(USetCommand<E> command) {
        if (command instanceof USet.AddCommand) {
            return doAdd(((USet.AddCommand<E>) command).getElement())? Option.of(command) : Option.none();
        } else if (command instanceof USet.RemoveCommand) {
            return doRemove(((USet.RemoveCommand<E>) command).getElement())? Option.of(command) : Option.none();
        }
        return Option.none();
    }


    // core functionality
    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public Iterator<E> iterator() {
        return new TwoPSetIterator();
    }

    @Override
    public boolean add(E value) {
        final boolean changed = doAdd(value);
        if (changed) {
            commands.onNext(new USet.AddCommand<>(crdtId, value));
        }
        return changed;
    }


    // implementation
    private boolean doAdd(E value) {
        return elements.add(value);
    }

    private boolean doRemove(E value) {
        return elements.remove(value);
    }

    private class TwoPSetIterator implements Iterator<E> {

        final Iterator<E> it = elements.iterator();
        E lastElement = null;

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public E next() {
            lastElement = it.next();
            return lastElement;
        }

        @Override
        public void remove() {
            it.remove();
            commands.onNext(new RemoveCommand<>(crdtId, lastElement));
        }
    }


    // commands
    @SuppressWarnings({"WeakerAccess", "unused"})
    public abstract static class USetCommand<E> extends CrdtCommand {
        protected USetCommand(String crdtId) {
            super(crdtId);
        }
    }

    public static final class AddCommand<E> extends USetCommand<E> {

        private final E element;

        AddCommand(String crdtId, E element) {
            super(crdtId);
            this.element = element;
        }

        E getElement() {
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


    public static final class RemoveCommand<E> extends USetCommand<E> {

        private final E element;

        RemoveCommand(String crdt, E element) {
            super(crdt);
            this.element = element;
        }

        E getElement() {
            return element;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            RemoveCommand<?> that = (RemoveCommand<?>) o;

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
