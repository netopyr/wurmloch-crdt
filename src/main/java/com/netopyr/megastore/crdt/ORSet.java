package com.netopyr.megastore.crdt;

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

    private static class Element<U> {
        private final U value;
        private final UUID uid = UUID.randomUUID();

        private Element(U value) {
            this.value = value;
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
