package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ORSetTest {

    // Set functionality

    @Test
    public void shouldAddElements() {
        // given:
        final ORSet<String> set = new ORSet<>("ID_1");

        // when:
        final boolean result1 = set.add("1");

        // then:
        assertThat(result1, is(true));

        // when:
        final boolean result2 = set.add("2");

        // then:
        assertThat(result2, is(true));

        // when:
        final boolean result3 = set.add("1");

        // then:
        assertThat(result3, is(false));
    }

    @Test
    public void shouldReturnSize() {
        // given:
        final ORSet<String> set = new ORSet<>("ID_1");

        // then:
        assertThat(set.size(), is(0));

        // when:
        set.add("1");

        // then:
        assertThat(set.size(), is(1));

        // when:
        set.add("1");
        set.add("2");
        set.add("3");

        // then:
        assertThat(set.size(), is(3));
    }

    @Test
    public void shouldIterate() {
        // given:
        final ORSet<String> set = new ORSet<>("ID_1");

        // then:
        assertThat(set.iterator().hasNext(), is(false));

        // when:
        set.add("1");
        final Iterator<String> it1 = set.iterator();

        // then:
        assertThat(it1.hasNext(), is(true));
        assertThat(it1.next(), is("1"));
        assertThat(it1.hasNext(), is(false));

        // when:
        set.add("1");
        set.add("2");
        set.add("3");
        final Iterator<String> it2 = set.iterator();

        // then:
        final Set<String> results = new HashSet<>();
        while (it2.hasNext()) {
            results.add(it2.next());
        }
        assertThat(results, contains("1", "2", "3"));
    }

    @Test
    public void shouldRemoveElements() {
        // given:
        final Set<String> expected = new HashSet<>();
        expected.addAll(Arrays.asList("1", "2", "3"));
        final ORSet<String> set = new ORSet<>("ID_1");
        set.addAll(expected);
        final Iterator<String> it = set.iterator();

        // when:
        final String e1 = it.next();
        it.remove();
        expected.remove(e1);

        // then:
        assertThat(set, equalTo(expected));

        // when:
        final String e2 = it.next();
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
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final ORSet<String> set = new ORSet<>("ID_1");
        set.subscribe(subscriber);

        // when:
        set.add("1");
        set.add("2");
        set.add("1");

        // then:
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new AddCommandMatcher<>(set.getCrdtId(), "1"),
                new AddCommandMatcher<>(set.getCrdtId(), "2"),
                new AddCommandMatcher<>(set.getCrdtId(), "1")
        ));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSendNotificationForRemoves() {
        // given:
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final ORSet<String> set = new ORSet<>("ID_1");
        set.subscribe(subscriber);

        set.add("1");
        set.add("1");

        // when:
        final Iterator<String> it = set.iterator();
        it.next();
        it.remove();

        // then:
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new AddCommandMatcher<>(set.getCrdtId(), "1"),
                new AddCommandMatcher<>(set.getCrdtId(), "1"),
                new RemoveCommandMatcher<>(set.getCrdtId(), "1", "1")
        ));
    }

    @Test
    public void shouldHandleAddCommands() {
        // given:
        final Processor<ORSet.ORSetCommand<String>, ORSet.ORSetCommand<String>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final ORSet<String> set = new ORSet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final ORSet.AddCommand<String> command1 = new ORSet.AddCommand<>(set.getCrdtId(), new ORSet.Element<>("1", UUID.randomUUID()));
        final ORSet.AddCommand<String> command2 = new ORSet.AddCommand<>(set.getCrdtId(), new ORSet.Element<>("2", UUID.randomUUID()));
        final ORSet.AddCommand<String> command3 = new ORSet.AddCommand<>(set.getCrdtId(), new ORSet.Element<>("1", UUID.randomUUID()));

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);
        inputStream.onNext(command3);

        // then:
        assertThat(set, hasSize(2));
        assertThat(subscriber.valueCount(), is(3));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }

    @Test
    public void shouldHandleRemoveCommands() {
        // given:
        final Processor<ORSet.ORSetCommand<String>, ORSet.ORSetCommand<String>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final ORSet<String> set = new ORSet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final ORSet.Element<String> elem1 = new ORSet.Element<>("1", UUID.randomUUID());
        final ORSet.Element<String> elem2 = new ORSet.Element<>("1", UUID.randomUUID());
        final Set<ORSet.Element<String>> elements = new HashSet<>(Arrays.asList(elem1, elem2));
        final ORSet.AddCommand<String> command1 = new ORSet.AddCommand<>(set.getCrdtId(), elem1);
        final ORSet.AddCommand<String> command2 = new ORSet.AddCommand<>(set.getCrdtId(), elem2);
        final ORSet.RemoveCommand<String> command3 = new ORSet.RemoveCommand<>(set.getCrdtId(), elements);

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);
        inputStream.onNext(command3);

        // then:
        assertThat(set, empty());
        assertThat(subscriber.valueCount(), is(3));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }

    @Test
    public void shouldHandleDuplicateCommands() {
        // given:
        final Processor<ORSet.ORSetCommand<String>, ORSet.ORSetCommand<String>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final ORSet<String> set = new ORSet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final ORSet.AddCommand<String> command = new ORSet.AddCommand<>(set.getCrdtId(), new ORSet.Element<>("1", UUID.randomUUID()));

        // when:
        inputStream.onNext(command);
        inputStream.onNext(command);

        // then:
        assertThat(set, hasSize(1));
        assertThat(subscriber.valueCount(), is(1));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }

    @Test
    public void shouldHandleRemoveCommandArrivesBeforeAddCommand() {

    }

    // Observable functionality

    private static class RemoveCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final List<T> values;

        @SafeVarargs
        private RemoveCommandMatcher(String crdtId, T... values) {
            super(String.format("RemoveCommandMatcher[crdtId=%s,values=%s]", crdtId, Arrays.toString(values)));
            this.crdtId = crdtId;
            this.values = Arrays.asList(values);
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean matches(Object o) {
            if (o instanceof ORSet.RemoveCommand) {
                final ORSet.RemoveCommand<T> command = (ORSet.RemoveCommand<T>) o;
                if (! command.getCrdtId().equals(crdtId)) {
                    return false;
                }
                for (final ORSet.Element<T> element : command.getElements()) {
                    if (! values.contains(element.getValue())) {
                        return false;
                    }
                }
                return true;
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
            if (o instanceof ORSet.AddCommand) {
                final ORSet.AddCommand command = (ORSet.AddCommand) o;
                return command.getCrdtId().equals(crdtId) && command.getElement().getValue().equals(value);
            }
            return false;
        }
    }
}
