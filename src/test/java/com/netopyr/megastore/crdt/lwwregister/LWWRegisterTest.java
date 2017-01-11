package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.replica.LocalReplica;
import org.testng.annotations.Test;

import static com.netopyr.caj.Caj.expect;

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
        expect(v0).to.be(null);

        // when
        register.set("Hello World");
        final String v1 = register.get();

        // then
        expect(v1).to.equal("Hello World");

        // when
        register.set("Goodbye World");
        final String v2 = register.get();

        // then
        expect(v2).to.equal("Goodbye World");
    }
}
