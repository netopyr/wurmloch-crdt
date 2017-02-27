package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.crdt.LWWRegister;
import com.netopyr.wurmloch.store.CrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LWWRegisterExample {

    @Test
    public void runLWWRegisterExample() {

        // create two CrdtStores and connect them
        final CrdtStore crdtStore1 = new CrdtStore("N_1");
        final CrdtStore crdtStore2 = new CrdtStore("N_2");
        crdtStore1.connect(crdtStore2);

        // create an LWW-Register and find the according replica in the second store
        final LWWRegister<String> replica1 = crdtStore1.createLWWRegister("ID_1");
        final LWWRegister<String> replica2 = crdtStore2.<String>findLWWRegister("ID_1").get();

        // set values in both replicas
        replica1.set("apple");
        replica2.set("banana");

        // the stores are connected, thus the last write wins
        assertThat(replica1.get(), is("banana"));
        assertThat(replica2.get(), is("banana"));


        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // add one entry to each replica
        replica1.set("strawberry");
        replica2.set("pear");

        // the stores are not connected, thus the changes have only local effects
        assertThat(replica1.get(), is("strawberry"));
        assertThat(replica2.get(), is("pear"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // the LWW-Register is synchronized automatically.
        // as the update happened concurrently, the update from the node with the larger ID wins
        assertThat(replica1.get(), is("pear"));
        assertThat(replica2.get(), is("pear"));

    }
}
