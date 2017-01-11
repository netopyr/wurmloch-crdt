package com.netopyr.megastore.crdt.orset;

import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.replica.Replica;
import io.reactivex.Observable;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class ORSetTest {

    // Set functionality

    @Test
    @SuppressWarnings("unchecked")
    public void shouldAddElements() {
        // given:
        final Replica replica = Mockito.mock(Replica.class);
        when(replica.onCommands(any())).thenReturn(Observable.empty());
        final ORSet<String> set = new ORSet<>(replica, "ID");

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
        final Replica replica = Mockito.mock(Replica.class);
        when(replica.onCommands(any())).thenReturn(Observable.empty());
        final ORSet<String> set = new ORSet<>(replica, "ID");

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
        final Replica replica = Mockito.mock(Replica.class);
        when(replica.onCommands(any())).thenReturn(Observable.empty());
        final ORSet<String> set = new ORSet<>(replica, "ID");

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
        final Replica replica = Mockito.mock(Replica.class);
        when(replica.onCommands(any())).thenReturn(Observable.empty());
        final Set<String> expected = new HashSet<>();
        expected.addAll(Arrays.asList("1", "2", "3"));
        final ORSet<String> set = new ORSet<>(replica, "ID");
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

    @Test
    public void shouldSendNotificationForAdds() {
        // given:
        final Replica replica = Mockito.mock(Replica.class);
        when(replica.onCommands(any())).thenReturn(Observable.empty());

        final ORSet<String> set = new ORSet<>(replica, "ID");
        final TestObserver<CrdtCommand> observer = new TestObserver<>();
        set.onCommand().subscribe(observer);

        // when:
        set.add("1");
        set.add("2");
        set.add("1");

        // then:
        observer.assertNotComplete();
        observer.assertNoErrors();
        assertThat(observer.values(), contains(
                new AddCommandMatcher<>(set.getId(), "1"),
                new AddCommandMatcher<>(set.getId(), "2"),
                new AddCommandMatcher<>(set.getId(), "1")
        ));
    }

    @Test
    public void shouldSendNotificationForRemoves() {
        // given:
        final Replica replica = Mockito.mock(Replica.class);
        when(replica.onCommands(any())).thenReturn(Observable.empty());

        final ORSet<String> set = new ORSet<>(replica, "ID");
        final TestObserver<CrdtCommand> observer = new TestObserver<>();
        set.onCommand().subscribe(observer);

        set.add("1");
        set.add("1");

        // when:
        final Iterator<String> it = set.iterator();
        it.next();
        it.remove();

        // then:
        observer.assertNotComplete();
        observer.assertNoErrors();
        assertThat(observer.values(), contains(
                new AddCommandMatcher<>(set.getId(), "1"),
                new AddCommandMatcher<>(set.getId(), "1"),
                new RemoveCommandMatcher<>(set.getId(), "1", "1")
        ));
    }

    @Test
    public void shouldHandleAddCommands() {
        // given:
        final Replica replica = Mockito.mock(Replica.class);
        final Subject<CrdtCommand> inputStream = ReplaySubject.create();
        when(replica.onCommands(any())).thenReturn(inputStream);

        final ORSet<String> set = new ORSet<>(replica, "ID");
        final TestObserver<CrdtCommand> observer = new TestObserver<>();
        set.onCommand().subscribe(observer);

        final AddCommand<String> command1 = new AddCommand<>(set.getId(), new Element<>("1", UUID.randomUUID()));
        final AddCommand<String> command2 = new AddCommand<>(set.getId(), new Element<>("2", UUID.randomUUID()));
        final AddCommand<String> command3 = new AddCommand<>(set.getId(), new Element<>("1", UUID.randomUUID()));

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);
        inputStream.onNext(command3);

        // then:
        assertThat(set, hasSize(2));
        observer.assertNotComplete();
        observer.assertNoErrors();
        observer.assertNoValues();
    }

    @Test
    public void shouldHandleRemoveCommands() {
        // given:
        final Replica replica = Mockito.mock(Replica.class);
        final Subject<CrdtCommand> inputStream = ReplaySubject.create();
        when(replica.onCommands(any())).thenReturn(inputStream);

        final ORSet<String> set = new ORSet<>(replica, "ID");
        final TestObserver<CrdtCommand> observer = new TestObserver<>();
        set.onCommand().subscribe(observer);

        final Element<String> elem1 = new Element<>("1", UUID.randomUUID());
        final Element<String> elem2 = new Element<>("1", UUID.randomUUID());
        final Set<Element<String>> elements = new HashSet<>(Arrays.asList(elem1, elem2));
        final AddCommand<String> command1 = new AddCommand<>(set.getId(), elem1);
        final AddCommand<String> command2 = new AddCommand<>(set.getId(), elem2);
        final RemoveCommand<String> command3 = new RemoveCommand<>(set.getId(), elements);

        // when:
        inputStream.onNext(command1);
        inputStream.onNext(command2);
        inputStream.onNext(command3);

        // then:
        assertThat(set, empty());
        observer.assertNotComplete();
        observer.assertNoErrors();
        observer.assertNoValues();
    }

    @Test
    public void shouldHandleDuplicateCommands() {
        // given:
        final Replica replica = Mockito.mock(Replica.class);
        final Subject<CrdtCommand> inputStream = ReplaySubject.create();
        when(replica.onCommands(any())).thenReturn(inputStream);

        final ORSet<String> set = new ORSet<>(replica, "ID");
        final TestObserver<CrdtCommand> observer = new TestObserver<>();
        set.onCommand().subscribe(observer);

        final AddCommand<String> command = new AddCommand<>(set.getId(), new Element<>("1", UUID.randomUUID()));

        // when:
        inputStream.onNext(command);
        inputStream.onNext(command);

        // then:
        assertThat(set, hasSize(1));
        observer.assertNotComplete();
        observer.assertNoErrors();
        observer.assertNoValues();
    }

    @Test
    public void shouldHandleRemoveCommandArrivesBeforeAddCommand() {

    }

    // Observable functionality

}
