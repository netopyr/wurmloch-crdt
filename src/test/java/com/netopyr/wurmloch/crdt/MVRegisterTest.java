package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.collection.Array;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

public class MVRegisterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String NODE_ID_3 = "N_3";
    private static final String CRDT_ID = "ID_1";

    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new MVRegister<>(null, CRDT_ID);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new MVRegister<String>(NODE_ID_1, null);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void subscribingToNullPublisherShouldThrow() {
        final MVRegister<String> register = new MVRegister<>(NODE_ID_1, CRDT_ID);
        register.subscribeTo(null);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void subscribingNullSubscriberShouldThrow() {
        final MVRegister<String> register = new MVRegister<>(NODE_ID_1, CRDT_ID);
        register.subscribe(null);
    }


    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID);
        register1.set("Hello World");
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID);

        // when
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // then
        assertThat(register2.get(), contains("Hello World"));
    }


    @Test
    public void itShouldGetAndSetValues() {
        // given
        final MVRegister<String> register = new MVRegister<>(NODE_ID_1, CRDT_ID);

        // when
        final Array<String> v0 = register.get();

        // then
        assertThat(v0, is(emptyIterable()));

        // when
        register.set("Hello World");
        final Array<String> v1 = register.get();

        // then
        assertThat(v1, contains("Hello World"));

        // when
        register.set("Goodbye World");
        final Array<String> v2 = register.get();

        // then
        assertThat(v2, contains("Goodbye World"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldSendCommandsOnUpdates() {
        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final MVRegister<String> register = new MVRegister<>(NODE_ID_1, CRDT_ID);
        register.subscribe(subscriber);

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(CRDT_ID, "Hello World")
        ));

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(CRDT_ID, "Hello World")
        ));

        // when
        register.set("Goodbye World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), containsInAnyOrder(
                new SetCommandMatcher<>(CRDT_ID, "Hello World"),
                new SetCommandMatcher<>(CRDT_ID, "Goodbye World")
        ));
    }


    @Test
    public void itShouldAcceptNewerValueFromReceivedCommands() {
        // given
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID);
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // when
        register1.set("Hello World");

        // then
        assertThat(register2.get(), contains("Hello World"));

        // when
        register2.set("Goodbye World");

        // then
        assertThat(register1.get(), contains("Goodbye World"));
    }


    @Test
    public void itShouldIgnoreOlderValueFromReceivedCommands() {
        // given
        final TestSubscriber<MVRegister.SetCommand<String>> outCommands1 = TestSubscriber.create();
        final TestSubscriber<MVRegister.SetCommand<String>> outCommands2 = TestSubscriber.create();
        final Processor<MVRegister.SetCommand<String>, MVRegister.SetCommand<String>> inCommands3 = ReplayProcessor.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID);
        register1.subscribe(outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID);
        register2.subscribe(outCommands2);
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);
        final MVRegister<String> register3 = new MVRegister<>(NODE_ID_3, CRDT_ID);
        register3.subscribeTo(inCommands3);


        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        final MVRegister.SetCommand<String> oldCommand = outCommands1.values().get(0);
        final MVRegister.SetCommand<String> newCommand = outCommands2.values().get(1);
        inCommands3.onNext(newCommand);
        inCommands3.onNext(oldCommand);

        // then
        assertThat(register3.get(), contains("Goodbye World"));
    }


    @Test
    public void itShouldAddOtherValueIfCommandsAreConcurrent() {
        // given
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID);

        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // then
        assertThat(register1.get(), containsInAnyOrder("Hello World", "Goodbye World"));
        assertThat(register2.get(), containsInAnyOrder("Hello World", "Goodbye World"));
    }


    @Test
    public void itShouldOverwriteConcurrentValues() {
        // given
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID);

        register1.set("Hello World");
        register2.set("Goodbye World");
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // when
        register1.set("42");

        // then
        assertThat(register1.get(), containsInAnyOrder("42"));
        assertThat(register2.get(), containsInAnyOrder("42"));
    }


    @Test
    public void itShouldOverwriteOnlyPartialCommandsFromReceivedCommand() {
        // given
        final TestSubscriber<MVRegister.SetCommand<String>> outCommands1 = TestSubscriber.create();
        final Processor<MVRegister.SetCommand<String>, MVRegister.SetCommand<String>> inCommands2 = ReplayProcessor.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID);
        register1.subscribe(outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID);
        register2.subscribeTo(inCommands2);

        register1.set("Hello World");
        register2.set("Goodbye World");
        inCommands2.onNext(outCommands1.values().get(0));

        // when
        register1.set("42");
        inCommands2.onNext(outCommands1.values().get(1));

        // then
        assertThat(register1.get(), containsInAnyOrder("42"));
        assertThat(register2.get(), containsInAnyOrder("42", "Goodbye World"));
    }

    private static class SetCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final T value;

        private SetCommandMatcher(String crdtId, T value) {
            super(String.format("SetMVCommandMatcher[crdtId=%s,value=%s]", crdtId, value));
            this.crdtId = crdtId;
            this.value = value;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof MVRegister.SetCommand) {
                final MVRegister.SetCommand command = (MVRegister.SetCommand) o;
                return Objects.equals(command.getCrdtId(), crdtId)
                        && Objects.equals(command.getEntry().getValue(), value)
                        && command.getEntry().getClock() != null;
            }
            return false;
        }
    }
}
