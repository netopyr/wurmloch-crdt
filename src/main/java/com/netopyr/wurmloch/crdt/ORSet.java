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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ORSet<T> extends AbstractSet<T> implements Crdt /*, ObservableSet<T> */ {

    private final String id;
    private final Set<Element<T>> elements = new HashSet<>();
    private final Set<Element<T>> tombstone = new HashSet<>();
    private final Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();


    public ORSet(String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
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
        return (nodeId, id, inCommands, outCommands) -> new ORSet<T>(id, inCommands, outCommands);
    }

    @Override
    public int size() {
        return doElements().size();
    }

    @Override
    public Iterator<T> iterator() {
        return new ORSetIterator();
    }

    @Override
    public boolean add(T value) {
        final boolean contained = doContains(value);
        prepareAdd(value);
        return ! contained;
    }


    private static <U> Predicate<Element<U>> matches(U value) {
        return element -> Objects.equals(value, element.getValue());
    }

    private synchronized boolean doContains(T value) {
        return elements.parallelStream().anyMatch(matches(value));
    }

    private synchronized Set<T> doElements() {
        return elements.parallelStream().map(Element::getValue).collect(Collectors.toSet());
    }

    private synchronized void prepareAdd(T value) {
        final Element<T> element = new Element<>(value, UUID.randomUUID());
        commands.onNext(new AddCommand<>(getId(), element));
        doAdd(element);
    }

    private synchronized void doAdd(Element<T> element) {
        elements.add(element);
        elements.removeAll(tombstone);
    }

    private synchronized void prepareRemove(T value) {
        final Set<Element<T>> removes = elements.parallelStream().filter(matches(value)).collect(Collectors.toSet());
        commands.onNext(new RemoveCommand<>(getId(), removes));
        doRemove(removes);
    }

    private synchronized void doRemove(Collection<Element<T>> removes) {
        elements.removeAll(removes);
        tombstone.addAll(removes);
    }

    @SuppressWarnings("unchecked")
    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (AddCommand.class.equals(clazz)) {
            doAdd(((AddCommand)command).getElement());
        } else if (RemoveCommand.class.equals(clazz)) {
            doRemove(((RemoveCommand)command).getElements());
        }
    }

    private class ORSetIterator implements Iterator<T> {

        final Iterator<T> it = doElements().iterator();
        T lastElement = null;

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            lastElement = it.next();
            return lastElement;
        }

        @Override
        public void remove() {
            it.remove();
            ORSet.this.prepareRemove(lastElement);
        }
    }


    static final class Element<T> {

        private final T value;
        private final UUID uuid;

        Element(T value, UUID uuid) {
            this.value = value;
            this.uuid = uuid;
        }

        T getValue() {
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


    static final class AddCommand<T> extends CrdtCommand {

        private final Element<T> element;

        AddCommand(String crdtId, Element<T> element) {
            super(crdtId);
            this.element = element;
        }

        Element<T> getElement() {
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


    static final class RemoveCommand<T> extends CrdtCommand {

        private final Set<Element<T>> elements;

        RemoveCommand(String crdt, Set<Element<T>> elements) {
            super(crdt);
            this.elements = elements;
        }

        Set<Element<T>> getElements() {
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
