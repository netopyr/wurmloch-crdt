package com.netopyr.megastore.crdt.lwwregister;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class LWWRegisterTest {

    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new LWWRegister<>(null, "ID_1", mock(Publisher.class), mock(Subscriber.class));
    }

    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new LWWRegister<String>("R_1", null, mock(Publisher.class), mock(Subscriber.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldSetAndGetValue() {
        final LWWRegister<String> register = new LWWRegister<>("R_1", "ID_1", mock(Publisher.class), mock(Subscriber.class));

        // when
        final String v0 = register.get();

        // then
        assertThat(v0, is(nullValue()));

        // when
        register.set("Hello World");
        final String v1 = register.get();

        // then
        assertThat(v1, is("Hello World"));

        // when
        register.set("Goodbye World");
        final String v2 = register.get();

        // then
        assertThat(v2, is("Goodbye World"));
    }
}
