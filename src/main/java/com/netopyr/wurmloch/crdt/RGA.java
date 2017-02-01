package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.vectorclock.StrictVectorClock;
import io.reactivex.processors.PublishProcessor;
import javaslang.Function4;
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

public class RGA<E> extends AbstractList<E> implements Crdt {

    private final String crtdId;
    private final Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();
    private final Vertex<E> start;

    private Map<StrictVectorClock, Vertex<E>> vertices;
    private Map<Vertex<E>, Vertex<E>> edges = HashMap.empty();
    private StrictVectorClock clock;
    private int size;

    public RGA(String nodeId, String crtdId, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        this.crtdId = Objects.requireNonNull(crtdId, "CrtdId must not be null");

        Objects.requireNonNull(nodeId, "NodeId must not be null");
        this.clock = new StrictVectorClock(nodeId);
        this.start = new Vertex<>(null, clock);
        this.vertices = HashMap.of(clock, start);

        inCommands.subscribe(new CrdtSubscriber(crtdId, this::processCommand));
        commands.subscribe(outCommands);
    }

    @Override
    public String getId() {
        return crtdId;
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return RGA::new;
    }

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
        commands.onNext(new RemoveCommand(crtdId, vertex.clock));
        doRemove(vertex);
    }

    private void doRemove(Vertex<E> vertex) {
        if (! vertex.removed) {
            vertex.removed = true;
            size--;
        }
    }

    private void prepareAddRight(Vertex<E> anchor, E value) {
        clock = clock.increment();
        commands.onNext(new AddRightCommand<>(crtdId, anchor.clock, value, clock));
        doAddRight(anchor, value, clock);
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

    @SuppressWarnings("unchecked")
    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (AddRightCommand.class.equals(clazz)) {

            final AddRightCommand<E> addRightCommand = (AddRightCommand<E>) command;
            final Option<Vertex<E>> anchor = findVertex(addRightCommand.anchorClock);
            clock = clock.merge(addRightCommand.newVertexClock);
            anchor.peek(
                    vertex -> doAddRight(vertex, addRightCommand.newVertexValue, addRightCommand.newVertexClock)
            );

        } else if (RemoveCommand.class.equals(clazz)) {

            final StrictVectorClock removedClock = ((RemoveCommand) command).clock;
            final Option<Vertex<E>> vertex = findVertex(removedClock);
            vertex.peek(this::doRemove);

        }
    }

    static final class Vertex<E> {

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

    static final class RemoveCommand extends CrdtCommand {

        private final StrictVectorClock clock;

        private RemoveCommand(String crtdId, StrictVectorClock clock) {
            super(crtdId);
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

    static final class AddRightCommand<E> extends CrdtCommand {

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
