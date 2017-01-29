package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.replica.LocalReplica;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class GSetExample {

    @SuppressWarnings("unchecked")
    @Test
    public static void runGSetExample() {

        final LocalReplica replica1 = new LocalReplica();
        final LocalReplica replica2 = new LocalReplica();
        replica1.connect(replica2);
        final GSet<String> gSetInReplica1 = replica1.createGSet("ID_1");
        final GSet<String> gSetInReplica2 = (GSet<String>) replica2.findCrdt("ID_1").get();

        gSetInReplica1.add("1a");
        gSetInReplica2.add("2a");

        assertThat(gSetInReplica1, containsInAnyOrder("1a", "2a"));
        assertThat(gSetInReplica2, containsInAnyOrder("1a", "2a"));

        replica1.disconnect(replica2);

        gSetInReplica1.add("1b");
        gSetInReplica2.add("2b");

        assertThat(gSetInReplica1, containsInAnyOrder("1a", "1b", "2a"));
        assertThat(gSetInReplica2, containsInAnyOrder("1a", "2a", "2b"));

        replica1.connect(replica2);

        assertThat(gSetInReplica1, containsInAnyOrder("1a", "1b", "2a", "2b"));
        assertThat(gSetInReplica2, containsInAnyOrder("1a", "1b", "2a", "2b"));
    }
}
