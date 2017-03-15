package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class USetTest {

    // Set functionality

    @Test
    public void shouldAddElements() {
        // given:
        final USet<UUID> set = new USet<>("ID_1");

        // when:
        final boolean result1 = set.add(UUID.randomUUID());

        // then:
        assertThat(result1, is(true));

        // when:
        final boolean result2 = set.add(UUID.randomUUID());

        // then:
        assertThat(result2, is(true));
    }

    @Test
    public void shouldReturnSize() {
        // given:
        final USet<UUID> set = new USet<>("ID_1");

        // then:
        assertThat(set.size(), is(0));

        // when:
        set.add(UUID.randomUUID());

        // then:
        assertThat(set.size(), is(1));

        // when:
        set.add(UUID.randomUUID());
        set.add(UUID.randomUUID());
        set.add(UUID.randomUUID());

        // then:
        assertThat(set.size(), is(4));
    }

    @Test
    public void shouldIterate() {
        // given:
        final USet<UUID> set = new USet<>("ID_1");
        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final UUID uuid3 = UUID.randomUUID();
        final UUID uuid4 = UUID.randomUUID();

        // then:
        assertThat(set.iterator().hasNext(), is(false));

        // when:
        set.add(uuid1);
        final Iterator<UUID> it1 = set.iterator();

        // then:
        assertThat(it1.hasNext(), is(true));
        assertThat(it1.next(), is(uuid1));
        assertThat(it1.hasNext(), is(false));

        // when:
        set.add(uuid2);
        set.add(uuid3);
        set.add(uuid4);
        final Iterator<UUID> it2 = set.iterator();

        // then:
        final Set<UUID> results = new HashSet<>();
        while (it2.hasNext()) {
            results.add(it2.next());
        }
        assertThat(results, containsInAnyOrder(uuid1, uuid2, uuid3, uuid4));
    }

    @Test
    public void shouldRemoveElements() {
        // given:
        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final UUID uuid3 = UUID.randomUUID();
        final Set<UUID> expected = new HashSet<>();
        expected.addAll(Arrays.asList(uuid1, uuid2, uuid3));
        final USet<UUID> set = new USet<>("ID_1");
        set.addAll(expected);
        final Iterator<UUID> it = set.iterator();

        // when:
        final UUID e1 = it.next();
        it.remove();
        expected.remove(e1);

        // then:
        assertThat(set, equalTo(expected));

        // when:
        final UUID e2 = it.next();
        it.remove();
        expected.remove(e2);

        // then:
        assertThat(set, equalTo(expected));

        // when:
        it.next();
        it.remove();

        // then:
        assertThat(set, empty());
    }


    // CRDT functionality

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSendNotificationForAdds() {
        // given:
        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final USet<UUID> set = new USet<>("ID_1");
        set.subscribe(subscriber);

        // when:
        set.add(uuid1);
        set.add(uuid2);

        // then:
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new AddCommandMatcher<>(set.getCrdtId(), uuid1),
                new AddCommandMatcher<>(set.getCrdtId(), uuid2)
        ));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSendNotificationForRemoves() {
        // given:
        final UUID uuid1 = UUID.randomUUID();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final USet<UUID> set = new USet<>("ID_1");
        set.subscribe(subscriber);

        set.add(uuid1);

        // when:
        final Iterator<UUID> it = set.iterator();
        it.next();
        it.remove();

        // then:
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new AddCommandMatcher<>(set.getCrdtId(), uuid1),
                new RemoveCommandMatcher<>(set.getCrdtId(), uuid1)
        ));
    }

    @Test
    public void shouldHandleAddCommands() {
        // given:
        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final Processor<USet.USetCommand<UUID>, USet.USetCommand<UUID>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final USet<UUID> set = new USet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final USet.AddCommand<UUID> command1 = new USet.AddCommand<>(set.getCrdtId(), uuid1);
        final USet.AddCommand<UUID> command2 = new USet.AddCommand<>(set.getCrdtId(), uuid2);

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);

        // then:
        assertThat(set, hasSize(2));
        assertThat(subscriber.valueCount(), is(2));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }

    @Test
    public void shouldHandleRemoveCommands() {
        // given:
        final UUID uuid1 = UUID.randomUUID();
        final Processor<USet.USetCommand<UUID>, USet.USetCommand<UUID>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final USet<UUID> set = new USet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final USet.AddCommand<UUID> command1 = new USet.AddCommand<>(set.getCrdtId(), uuid1);
        final USet.RemoveCommand<UUID> command2 = new USet.RemoveCommand<>(set.getCrdtId(), uuid1);

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);

        // then:
        assertThat(set, empty());
        assertThat(subscriber.valueCount(), is(2));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }

    // Observable functionality

    private static class RemoveCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final T value;

        private RemoveCommandMatcher(String crdtId, T value) {
            super(String.format("RemoveCommandMatcher[crdtId=%s,values=%s]", crdtId, value));
            this.crdtId = crdtId;
            this.value = value;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean matches(Object o) {
            if (o instanceof USet.RemoveCommand) {
                final USet.RemoveCommand<T> command = (USet.RemoveCommand<T>) o;
                return command.getCrdtId().equals(crdtId) && command.getElement().equals(value);
            }
            return false;
        }
    }

    private static class AddCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final T value;

        private AddCommandMatcher(String crdtId, T value) {
            super(String.format("AddCommandMatcher[crdtId=%s,elementValue=%s]", crdtId, value));
            this.crdtId = crdtId;
            this.value = value;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof USet.AddCommand) {
                final USet.AddCommand command = (USet.AddCommand) o;
                return command.getCrdtId().equals(crdtId) && command.getElement().equals(value);
            }
            return false;
        }
    }
}
