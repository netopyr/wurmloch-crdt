package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.store.CrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class GSetExample {

    @Test
    public void runGSetExample() {

        // create two CrdtStores and connect them
        final CrdtStore crdtStore1 = new CrdtStore();
        final CrdtStore crdtStore2 = new CrdtStore();
        crdtStore1.connect(crdtStore2);

        // create a G-Set and find the according replica in the second store
        final GSet<String> replica1 = crdtStore1.createGSet("ID_1");
        final GSet<String> replica2 = crdtStore2.<String>findGSet("ID_1").get();

        // add one entry to each replica
        replica1.add("apple");
        replica2.add("banana");

        // the stores are connected, thus the G-Set is automatically synchronized
        assertThat(replica1, containsInAnyOrder("apple", "banana"));
        assertThat(replica2, containsInAnyOrder("apple", "banana"));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // add one entry to each replica
        replica1.add("strawberry");
        replica2.add("pear");

        // the stores are not connected, thus the changes have only local effects
        assertThat(replica1, containsInAnyOrder("apple", "banana", "strawberry"));
        assertThat(replica2, containsInAnyOrder("apple", "banana", "pear"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // the G-Set is synchronized automatically and contains now all elements
        assertThat(replica1, containsInAnyOrder("apple", "banana", "strawberry", "pear"));
        assertThat(replica2, containsInAnyOrder("apple", "banana", "strawberry", "pear"));

    }
}
