package com.netopyr.megastore.crdt;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import rx.Observable;

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ORSet<T> extends AbstractSet<T> implements ObservableSet<T> {

    private final Set<Element<T>> elements = new HashSet<>();
    private final Set<Element<T>> tombstone = new HashSet<>();

    public Observable<Change> onChange() {
        return null;
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
        final boolean result = doContains(value);
        doAdd(value);
        return result;
    }

    private static <U> Predicate<Element<U>> matches(U value) {
        return element -> value == null ? element.value == null : value.equals(element.value);
    }

    private synchronized boolean doContains(T value) {
        return elements.parallelStream().anyMatch(matches(value));
    }

    private synchronized Set<T> doElements() {
        return elements.parallelStream().map(element -> element.value).collect(Collectors.toCollection(HashSet::new));
    }

    private synchronized void doAdd(T value) {
        final Element<T> element = new Element<>(value);
        elements.add(element);
        elements.removeAll(tombstone);
    }

    private synchronized void doRemove(T value) {
        final Set<Element<T>> remove = elements.parallelStream().filter(matches(value)).collect(Collectors.toSet());
        elements.removeAll(remove);
        tombstone.addAll(remove);
    }

    private final static class Element<U> {
        private final U value;
        private final UUID uid;

        private Element(U value, UUID uid) {
            this.value = value;
            this.uid = uid;
        }
        private Element(U value) {
            this(value, UUID.randomUUID());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Element<?> element = (Element<?>) o;

            return new EqualsBuilder()
                    .append(value, element.value)
                    .append(uid, element.uid)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(value)
                    .append(uid)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("value", value)
                    .append("uid", uid)
                    .toString();
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
            ORSet.this.doRemove(lastElement);
        }
    }
}
