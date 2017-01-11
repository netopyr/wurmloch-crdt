package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.replica.LocalReplica;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LWWRegisterTest {

    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new LWWRegister<>(null, "ID_1");
    }

    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new LWWRegister<>(new LocalReplica(), null);
    }

    @Test
    public void shouldSetAndGetValue() {
        final LWWRegister<String> register = new LWWRegister<>(new LocalReplica(), "ID_1");

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
