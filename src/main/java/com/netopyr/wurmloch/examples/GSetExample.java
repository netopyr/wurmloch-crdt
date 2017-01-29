package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.store.LocalCrdtStore;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

public class GSetExample {

    public static void main(String[] args) {

        final LocalCrdtStore replica1 = new LocalCrdtStore();
        final LocalCrdtStore replica2 = new LocalCrdtStore();
        replica1.connect(replica2);
        final GSet<String> gSetInReplica1 = replica1.createGSet("ID_1");
        final GSet<String> gSetInReplica2 = replica2.<String>findGSet("ID_1").get();

        gSetInReplica1.add("1a");
        gSetInReplica2.add("2a");

        MatcherAssert.assertThat(gSetInReplica1, Matchers.containsInAnyOrder("1a", "2a"));
        MatcherAssert.assertThat(gSetInReplica2, Matchers.containsInAnyOrder("1a", "2a"));

        replica1.disconnect(replica2);

        gSetInReplica1.add("1b");
        gSetInReplica2.add("2b");

        MatcherAssert.assertThat(gSetInReplica1, Matchers.containsInAnyOrder("1a", "1b", "2a"));
        MatcherAssert.assertThat(gSetInReplica2, Matchers.containsInAnyOrder("1a", "2a", "2b"));

        replica1.connect(replica2);

        MatcherAssert.assertThat(gSetInReplica1, Matchers.containsInAnyOrder("1a", "1b", "2a", "2b"));
        MatcherAssert.assertThat(gSetInReplica2, Matchers.containsInAnyOrder("1a", "1b", "2a", "2b"));
    }
}
