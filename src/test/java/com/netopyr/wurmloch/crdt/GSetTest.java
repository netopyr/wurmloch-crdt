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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class GSetTest {

    // Set functionality

    @Test
    public void shouldAddElements() {
        // given:
        final GSet<String> set = new GSet<>("ID_1");

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
        final GSet<String> set = new GSet<>("ID_1");

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
        final GSet<String> set = new GSet<>("ID_1");

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
        assertThat(results, containsInAnyOrder("1", "2", "3"));
    }

    @Test (expectedExceptions = UnsupportedOperationException.class)
    public void shouldRemoveElements() {
        // given:
        final Set<String> expected = new HashSet<>();
        expected.addAll(Arrays.asList("1", "2", "3"));
        final GSet<String> set = new GSet<>("ID_1");
        set.addAll(expected);
        final Iterator<String> it = set.iterator();

        // when:
        it.next();
        it.remove();
    }


    // CRDT functionality

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSendNotificationForAdds() {
        // given:
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final GSet<String> set = new GSet<>("ID_1");
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

    @Test
    public void shouldHandleAddCommands() {
        // given:
        final Processor<GSet.AddCommand<String>, GSet.AddCommand<String>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final GSet<String> set = new GSet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final GSet.AddCommand<String> command1 = new GSet.AddCommand<>(set.getCrdtId(), "1");
        final GSet.AddCommand<String> command2 = new GSet.AddCommand<>(set.getCrdtId(), "2");
        final GSet.AddCommand<String> command3 = new GSet.AddCommand<>(set.getCrdtId(), "1");

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);
        inputStream.onNext(command3);

        // then:
        assertThat(set, hasSize(2));
        assertThat(subscriber.valueCount(), is(2));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }

    @Test
    public void shouldHandleDuplicateCommands() {
        // given:
        final Processor<GSet.AddCommand<String>, GSet.AddCommand<String>> inputStream = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final GSet<String> set = new GSet<>("ID_1");
        set.subscribeTo(inputStream);
        set.subscribe(subscriber);

        final GSet.AddCommand<String> command = new GSet.AddCommand<>(set.getCrdtId(), "1");

        // when:
        inputStream.onNext(command);
        inputStream.onNext(command);

        // then:
        assertThat(set, hasSize(1));
        assertThat(subscriber.valueCount(), is(1));
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
    }


    // Observable functionality

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
            if (o instanceof GSet.AddCommand) {
                final GSet.AddCommand command = (GSet.AddCommand) o;
                return command.getCrdtId().equals(crdtId) && command.getElement().equals(value);
            }
            return false;
        }
    }
}
