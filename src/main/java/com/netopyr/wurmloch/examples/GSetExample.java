package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.store.LocalCrdtStore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

public class GSetExample {

    public static void main(String[] args) {

        // create two LocalCrdtStores and connect them
        final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
        crdtStore1.connect(crdtStore2);

        // create a G-Set and find the according replica in the second store
        final GSet<String> replica1 = crdtStore1.createGSet("ID_1");
        final GSet<String> replica2 = crdtStore2.<String>findGSet("ID_1").get();

        // add one entry to each replica
        replica1.add("apple");
        replica2.add("banana");

        // the stores are connected, thus the G-Set is automatically synchronized
        MatcherAssert.assertThat(replica1, Matchers.containsInAnyOrder("apple", "banana"));
        MatcherAssert.assertThat(replica2, Matchers.containsInAnyOrder("apple", "banana"));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // add one entry to each replica
        replica1.add("strawberry");
        replica2.add("pear");

        // the stores are not connected, thus the changes have only local effects
        MatcherAssert.assertThat(replica1, Matchers.containsInAnyOrder("apple", "banana", "strawberry"));
        MatcherAssert.assertThat(replica2, Matchers.containsInAnyOrder("apple", "banana", "pear"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // the G-Set is synchronized automatically and contains now all elements
        MatcherAssert.assertThat(replica1, Matchers.containsInAnyOrder("apple", "banana", "strawberry", "pear"));
        MatcherAssert.assertThat(replica2, Matchers.containsInAnyOrder("apple", "banana", "strawberry", "pear"));

    }
}
