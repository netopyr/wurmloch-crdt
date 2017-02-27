package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.crdt.RGA;
import com.netopyr.wurmloch.store.LocalCrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class RGAExample {

    @Test
    public void runRGAExample() {

        // create two LocalCrdtStores and connect them
        final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
        crdtStore1.connect(crdtStore2);

        // create a G-Set and find the according replica in the second store
        final RGA<String> replica1 = crdtStore1.createRGA("ID_1");
        final RGA<String> replica2 = crdtStore2.<String>findRGA("ID_1").get();

        // add one entry to each replica
        replica1.add("apple");
        replica2.add("banana");

        // the stores are connected, thus the RGA is automatically synchronized
        assertThat(replica1, contains("apple", "banana"));
        assertThat(replica2, contains("apple", "banana"));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // add one entry to each replica
        replica1.remove("banana");
        replica2.add(1, "strawberry");

        // the stores are not connected, thus the changes have only local effects
        assertThat(replica1, containsInAnyOrder("apple"));
        assertThat(replica2, containsInAnyOrder("apple", "strawberry", "banana"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // the RGA is synchronized automatically
        assertThat(replica1, containsInAnyOrder("apple", "strawberry"));
        assertThat(replica2, containsInAnyOrder("apple", "strawberry"));

        // disconnect the stores
        crdtStore1.disconnect(crdtStore2);

        // set() is not supported in an RGA
        // if we try to simulate with a remove and add, we can see the problem
        replica1.remove(0);
        replica1.add("pear");
        replica2.remove(0);
        replica2.add("orange");

        // the first entry has been replaced
        assertThat(replica1, containsInAnyOrder("pear", "strawberry"));
        assertThat(replica2, containsInAnyOrder("orange", "strawberry"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // we have actually added two elements, the RGA keeps both
        assertThat(replica1, containsInAnyOrder("orange", "pear", "strawberry"));
        assertThat(replica2, containsInAnyOrder("orange", "pear", "strawberry"));

    }
}
