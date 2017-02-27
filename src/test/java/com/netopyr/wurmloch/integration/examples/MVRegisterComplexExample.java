package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.crdt.MVRegister;
import com.netopyr.wurmloch.store.LocalCrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class MVRegisterComplexExample {

    @Test
    public void runMVRegisterComplexExample() {

        // create three LocalCrdtStores and connect them
        final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore3 = new LocalCrdtStore();
        crdtStore2.connect(crdtStore1);
        crdtStore2.connect(crdtStore3);

        // create an MV-Register and find the according replica in the other stores
        final MVRegister<String> replica1 = crdtStore1.createMVRegister("ID_1");
        final MVRegister<String> replica2 = crdtStore2.<String>findMVRegister("ID_1").get();
        final MVRegister<String> replica3 = crdtStore3.<String>findMVRegister("ID_1").get();

        // disconnect store 2 and 3
        crdtStore2.disconnect(crdtStore3);

        // set some values
        replica1.set("apple");
        replica3.set("banana");

        // store 1 and 2 contain "apple", store 3 contains "banana"
        assertThat(replica1.get(), containsInAnyOrder("apple"));
        assertThat(replica2.get(), containsInAnyOrder("apple"));
        assertThat(replica3.get(), containsInAnyOrder("banana"));

        // disconnect store 1 and 2 and connect store 2 and 3 instead
        crdtStore2.disconnect(crdtStore1);
        crdtStore2.connect(crdtStore3);

        // set the register in store 1 to "strawberry"
        replica1.set("strawberry");

        // store 1 still contains "strawberry" only
        // store 2 and 3 are synchronized and contain "apple" and "banana", because these updates happened concurrently
        assertThat(replica1.get(), containsInAnyOrder("strawberry"));
        assertThat(replica2.get(), containsInAnyOrder("apple", "banana"));
        assertThat(replica3.get(), containsInAnyOrder("apple", "banana"));

        // connect all stores again
        crdtStore2.connect(crdtStore1);

        // the result is not simply the union of all values
        // "apple" was overridden by "strawberry", therefore it disappears now
        // "banana" and "strawberry" were set concurrently, thus both values are still in the result
        assertThat(replica1.get(), containsInAnyOrder("banana", "strawberry"));
        assertThat(replica2.get(), containsInAnyOrder("banana", "strawberry"));
        assertThat(replica3.get(), containsInAnyOrder("banana", "strawberry"));

    }
}
