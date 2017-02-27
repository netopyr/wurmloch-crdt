package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.vectorclock.StrictVectorClock;
import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.control.Option;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.AbstractList;
import java.util.Objects;

public class RGA<E> extends AbstractList<E> implements Crdt<RGA<E>, RGA.RGACommand<E>> {

    private final String crdtId;
    private final Processor<RGACommand<E>, RGACommand<E>> commands = ReplayProcessor.create();
    private final Vertex<E> start;

    private Map<StrictVectorClock, Vertex<E>> vertices;
    private Map<Vertex<E>, Vertex<E>> edges = HashMap.empty();
    private StrictVectorClock clock;
    private int size;


    // constructor
    public RGA(String nodeId, String crdtId) {
        this.crdtId = Objects.requireNonNull(crdtId, "CrtdId must not be null");

        Objects.requireNonNull(nodeId, "NodeId must not be null");
        this.clock = new StrictVectorClock(nodeId);
        this.start = new Vertex<>(null, clock);
        this.vertices = HashMap.of(clock, start);
    }


    // crdt
    @Override
    public String getCrdtId() {
        return crdtId;
    }

    @Override
    public void subscribe(Subscriber<? super RGACommand<E>> subscriber) {
        commands.subscribe(subscriber);
    }

    @Override
    public void subscribeTo(Publisher<? extends RGACommand<E>> publisher) {
        Flowable.fromPublisher(publisher).onTerminateDetach().subscribe(command -> {
            final Option<RGACommand<E>> newCommand = processCommand(command);
            newCommand.peek(commands::onNext);
        });
    }

    private Option<RGACommand<E>> processCommand(RGACommand<E> command) {
        if (command instanceof AddRightCommand) {
            final AddRightCommand<E> addRightCommand = (AddRightCommand<E>) command;
            if (findVertex(addRightCommand.newVertexClock).isEmpty()) {
                final Option<Vertex<E>> anchor = findVertex(addRightCommand.anchorClock);
                clock = clock.merge(addRightCommand.newVertexClock);
                anchor.peek(
                        vertex -> doAddRight(vertex, addRightCommand.newVertexValue, addRightCommand.newVertexClock)
                );
                return Option.of(command);
            }

        } else if (command instanceof RemoveCommand) {
            final StrictVectorClock removedClock = ((RemoveCommand) command).clock;
            final Option<Vertex<E>> vertex = findVertex(removedClock);
            return vertex.map(this::doRemove).flatMap(result -> result? Option.of(command) : Option.none());
        }

        return Option.none();
    }


    // core functionality
    @Override
    public E get(int index) {
        return findVertex(index).value;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void add(int index, E element) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException();
        }
        final Vertex<E> anchor = index == 0 ? start : findVertex(index - 1);
        prepareAddRight(anchor, element);
    }

    @Override
    public E remove(int index) {
        final Vertex<E> vertex = findVertex(index);
        prepareRemove(vertex);
        return vertex.value;
    }


    // implementation
    private Vertex<E> findVertex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        Vertex<E> vertex = start;
        for (int i = 0; i <= index; i++) {
            do {
                vertex = edges.get(vertex).get();
            } while (vertex.removed);
        }
        return vertex;
    }

    private Option<Vertex<E>> findVertex(StrictVectorClock clock) {
        return vertices.get(clock);
    }

    private void prepareRemove(Vertex<E> vertex) {
        commands.onNext(new RemoveCommand<>(crdtId, vertex.clock));
        doRemove(vertex);
    }

    private boolean doRemove(Vertex<E> vertex) {
        if (! vertex.removed) {
            vertex.removed = true;
            size--;
            return true;
        }
        return false;
    }

    private void prepareAddRight(Vertex<E> anchor, E value) {
        clock = clock.increment();
        doAddRight(anchor, value, clock);
        commands.onNext(new AddRightCommand<>(crdtId, anchor.clock, value, clock));
    }

    private void doAddRight(Vertex<E> l, E value, StrictVectorClock clock) {
        Option<Vertex<E>> r = successor(l);
        while (r.isDefined() && (clock.compareTo(r.get().clock) < 0)) {
            l = r.get();
            r = successor(l);
        }
        final Vertex<E> w = new Vertex<>(value, clock);
        vertices = vertices.put(clock, w);
        size++;
        edges = edges.put(l, w);
        if (r.isDefined()) {
            edges = edges.put(w, r.get());
        }
    }

    private Option<Vertex<E>> successor(Vertex<E> vertex) {
        return edges.get(vertex);
    }

    private static final class Vertex<E> {

        private final E value;
        private final StrictVectorClock clock;

        private boolean removed;

        private Vertex(E value, StrictVectorClock clock) {
            this.value = value;
            this.clock = clock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Vertex<?> vertex = (Vertex<?>) o;

            return Objects.equals(clock, vertex.clock);
        }

        @Override
        public int hashCode() {
            return clock.hashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("value", value)
                    .append("clock", clock)
                    .append("removed", removed)
                    .toString();
        }
    }


    // commands
    @SuppressWarnings({"WeakerAccess", "unused"})
    public abstract static class RGACommand<E> extends CrdtCommand {
        protected RGACommand(String crdtId) {
            super(crdtId);
        }
    }

    public static final class RemoveCommand<E> extends RGACommand<E> {

        private final StrictVectorClock clock;

        private RemoveCommand(String crdtId, StrictVectorClock clock) {
            super(crdtId);
            this.clock = clock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            RemoveCommand that = (RemoveCommand) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(clock, that.clock)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(clock)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("clock", clock)
                    .toString();
        }
    }

    public static final class AddRightCommand<E> extends RGACommand<E> {

        private final StrictVectorClock anchorClock;
        private final E newVertexValue;
        private final StrictVectorClock newVertexClock;

        private AddRightCommand(String crdtId, StrictVectorClock anchorClock, E newVertexValue, StrictVectorClock newVertexClock) {
            super(crdtId);
            this.anchorClock = Objects.requireNonNull(anchorClock, "AnchorClock must not be null");
            this.newVertexValue = Objects.requireNonNull(newVertexValue, "NewVertexValue must not be null");
            this.newVertexClock = Objects.requireNonNull(newVertexClock, "NewVertexClock must not be null");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            AddRightCommand<?> that = (AddRightCommand<?>) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(anchorClock, that.anchorClock)
                    .append(newVertexValue, that.newVertexValue)
                    .append(newVertexClock, that.newVertexClock)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(anchorClock)
                    .append(newVertexValue)
                    .append(newVertexClock)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("anchorClock", anchorClock)
                    .append("newVertexValue", newVertexValue)
                    .append("newVertexClock", newVertexClock)
                    .toString();
        }
    }
}
