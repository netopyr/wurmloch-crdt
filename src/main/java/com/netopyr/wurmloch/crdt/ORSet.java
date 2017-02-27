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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ORSet<E> extends AbstractSet<E> implements Crdt<ORSet<E>, ORSet.ORSetCommand<E>> /*, ObservableSet<E> */ {

    // fields
    private final String crdtId;
    private final Set<Element<E>> elements = new HashSet<>();
    private final Set<Element<E>> tombstone = new HashSet<>();
    private final Processor<ORSetCommand<E>, ORSetCommand<E>> commands = ReplayProcessor.create();


    // constructor
    public ORSet(String crdtId) {
        this.crdtId = Objects.requireNonNull(crdtId, "Id must not be null");
    }


    // crdt
    @Override
    public String getCrdtId() {
        return crdtId;
    }

    @Override
    public void subscribe(Subscriber<? super ORSetCommand<E>> subscriber) {
        commands.subscribe(subscriber);
    }

    @Override
    public void subscribeTo(Publisher<? extends ORSetCommand<E>> publisher) {
        Flowable.fromPublisher(publisher).onTerminateDetach().subscribe(command -> {
            final Option<ORSetCommand<E>> newCommand = processCommand(command);
            newCommand.peek(commands::onNext);

        });
    }

    private Option<ORSetCommand<E>> processCommand(ORSetCommand<E> command) {
        if (command instanceof AddCommand) {
            return doAdd(((AddCommand<E>) command).getElement())? Option.of(command) : Option.none();
        } else if (command instanceof RemoveCommand) {
            return doRemove(((RemoveCommand<E>) command).getElements())? Option.of(command) : Option.none();
        }
        return Option.none();
    }


    // core functionality
    @Override
    public int size() {
        return doElements().size();
    }

    @Override
    public Iterator<E> iterator() {
        return new ORSetIterator();
    }

    @Override
    public boolean add(E value) {
        final boolean contained = doContains(value);
        prepareAdd(value);
        return !contained;
    }


    // implementation
    private static <U> Predicate<Element<U>> matches(U value) {
        return element -> Objects.equals(value, element.getValue());
    }

    private synchronized boolean doContains(E value) {
        return elements.parallelStream().anyMatch(matches(value));
    }

    private synchronized Set<E> doElements() {
        return elements.parallelStream().map(Element::getValue).collect(Collectors.toSet());
    }

    private synchronized void prepareAdd(E value) {
        final Element<E> element = new Element<>(value, UUID.randomUUID());
        commands.onNext(new AddCommand<>(getCrdtId(), element));
        doAdd(element);
    }

    private synchronized boolean doAdd(Element<E> element) {
        return (elements.add(element) | elements.removeAll(tombstone)) && (!tombstone.contains(element));
    }

    private synchronized void prepareRemove(E value) {
        final Set<Element<E>> removes = elements.parallelStream().filter(matches(value)).collect(Collectors.toSet());
        commands.onNext(new RemoveCommand<>(getCrdtId(), removes));
        doRemove(removes);
    }

    private synchronized boolean doRemove(Collection<Element<E>> removes) {
        return elements.removeAll(removes) | tombstone.addAll(removes);
    }

    private class ORSetIterator implements Iterator<E> {

        final Iterator<E> it = doElements().iterator();
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
            ORSet.this.prepareRemove(lastElement);
        }
    }


    public static final class Element<E> {

        private final E value;
        private final UUID uuid;

        Element(E value, UUID uuid) {
            this.value = value;
            this.uuid = uuid;
        }

        E getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Element<?> element = (Element<?>) o;

            return new EqualsBuilder()
                    .append(value, element.value)
                    .append(uuid, element.uuid)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(value)
                    .append(uuid)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("value", value)
                    .append("uuid", uuid)
                    .toString();
        }
    }


    // commands
    @SuppressWarnings({"WeakerAccess", "unused"})
    public abstract static class ORSetCommand<E> extends CrdtCommand {
        protected ORSetCommand(String crdtId) {
            super(crdtId);
        }
    }

    public static final class AddCommand<E> extends ORSetCommand<E> {

        private final Element<E> element;

        AddCommand(String crdtId, Element<E> element) {
            super(crdtId);
            this.element = element;
        }

        Element<E> getElement() {
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


    public static final class RemoveCommand<E> extends ORSetCommand<E> {

        private final Set<Element<E>> elements;

        RemoveCommand(String crdt, Set<Element<E>> elements) {
            super(crdt);
            this.elements = elements;
        }

        Set<Element<E>> getElements() {
            return elements;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            RemoveCommand<?> that = (RemoveCommand<?>) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(elements, that.elements)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(elements)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("crdtId", getCrdtId())
                    .append("elements", elements)
                    .toString();
        }
    }
}
