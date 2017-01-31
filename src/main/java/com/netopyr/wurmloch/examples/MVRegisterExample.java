package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.MVRegister;
import com.netopyr.wurmloch.store.LocalCrdtStore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class MVRegisterExample {

    public static void main(String[] args) {

        // create two LocalCrdtStores and connect them
        final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
        crdtStore1.connect(crdtStore2);

        // create an MV-Register and find the according replica in the second store
        final MVRegister<String> replica1 = crdtStore1.createMVRegister("ID_1");
        final MVRegister<String> replica2 = crdtStore2.<String>findMVRegister("ID_1").get();

        // set values in both replicas
        replica1.set("apple");
        replica2.set("banana");

        // the stores are connected, thus we can determine the order of both writes
        // the latter write overrides the previous one
        assertThat(replica1.get(), contains("banana"));
        assertThat(replica2.get(), contains("banana"));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // change the value in both replicas
        replica1.set("strawberry");
        replica2.set("pear");

        // the stores are not connected, thus the changes have only local effects
        assertThat(replica1.get(), contains("strawberry"));
        assertThat(replica2.get(), contains("pear"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // as the update happened concurrently, we cannot determine an order and both values are kept
        assertThat(replica1.get(), containsInAnyOrder("strawberry", "pear"));
        assertThat(replica2.get(), containsInAnyOrder("strawberry", "pear"));

        // update the value one more time
        replica2.set("orange");

        // the last update was clearly after the concurrent ones, therefore both replicas contain the last value only
        assertThat(replica1.get(), contains("orange"));
        assertThat(replica2.get(), contains("orange"));
    }
}
