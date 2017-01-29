package com.netopyr.wurmloch.integration;

import com.netopyr.wurmloch.crdt.ORSet;
import com.netopyr.wurmloch.store.LocalCrdtStore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class LocalORSetTest {

    private LocalCrdtStore replica2;
    private LocalCrdtStore replica3;

    private ORSet<String> orSet1;
    private ORSet<String> orSet2;
    private ORSet<String> orSet3;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setUp() {
        final String ID = "TestORSet";
        final LocalCrdtStore replica1 = new LocalCrdtStore("ID_1");
        replica2 = new LocalCrdtStore("ID_2");
        replica3 = new LocalCrdtStore("ID_3");

        replica1.connect(replica2);
        replica2.connect(replica3);

        orSet1 = replica1.createORSet(ID);
        orSet2 = (ORSet<String>) replica2.findCrdt(ID).get();
        orSet3 = (ORSet<String>) replica3.findCrdt(ID).get();
    }

    @Test
    public void shouldSynchronizeSingleAdd() {
        // given:
        final String element = "Hello World";
        replica2.disconnect(replica3);

        // when:
        orSet1.add(element);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, empty());

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));
    }

    @Test
    public void shouldSynchronizeSingleDelete() {
        // given:
        final String element = "Hello World";
        orSet1.add(element);
        replica2.disconnect(replica3);

        // when:
        orSet1.remove(element);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, contains(element));

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, empty());
    }

    @Test
    public void shouldSynchronizeConcurrentAddsOfSameElement() {
        // given:
        final String element = "Hello World";
        replica2.disconnect(replica3);

        // when:
        orSet1.add(element);
        orSet3.add(element);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));
    }

    @Test
    public void shouldSynchronizeConcurrentAddsOfDifferentElements() {
        // given:
        final String element1 = "Hello World";
        final String element2 = "Good Bye";
        replica2.disconnect(replica3);

        // when:
        orSet1.add(element1);
        orSet3.add(element2);

        // then:
        assertThat(orSet1, contains(element1));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, contains(element2));

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element1, element2));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));

    }

    @Test
    public void shouldSynchronizeConcurrentDeletesOfSameElement() {
        // given:
        final String element = "Hello World";
        orSet1.add(element);
        replica2.disconnect(replica3);

        // when:
        orSet1.remove(element);
        orSet3.remove(element);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, empty());

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, empty());
    }

    @Test
    public void shouldSynchronizeConcurrentDeletesOfDifferentElements() {
        // given:
        final String element1 = "Hello World";
        final String element2 = "Good Bye";
        orSet1.add(element1);
        orSet1.add(element2);
        replica2.disconnect(replica3);

        // when:
        orSet1.remove(element1);
        orSet3.remove(element2);

        // then:
        assertThat(orSet1, contains(element2));
        assertThat(orSet2, equalTo(orSet2));
        assertThat(orSet3, contains(element1));

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, empty());
    }

    @Test
    public void shouldSynchronizeConcurrentAddAndDeleteDifferentElements() {
        // given:
        final String element1 = "Hello World";
        final String element2 = "Good Bye";
        orSet1.add(element1);
        replica2.disconnect(replica3);

        // when:
        orSet1.remove(element1);
        orSet3.add(element2);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, contains(element1, element2));

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element2));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));

        // when:
        replica2.disconnect(replica3);
        orSet1.add(element1);
        orSet3.remove(element2);

        // then:
        assertThat(orSet1, contains(element1, element2));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, empty());

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element1));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));
    }

    @Test
    public void shouldSynchronizeConcurrentAddAndDeleteSameElement() {
        // given:
        final String element = "Hello World";
        replica2.disconnect(replica3);
        orSet1.add(element);

        // when:
        orSet1.remove(element);
        orSet3.add(element);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, contains(element));

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));

        // when:
        replica2.disconnect(replica3);
        orSet1.remove(element);

        // then:
        assertThat(orSet1, empty());
        assertThat(orSet2, empty());
        assertThat(orSet3, contains(element));

        // when:
        orSet1.add(element);
        orSet3.remove(element);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, empty());

        // when:
        replica2.connect(replica3);

        // then:
        assertThat(orSet1, contains(element));
        assertThat(orSet2, equalTo(orSet1));
        assertThat(orSet3, equalTo(orSet1));
    }
}
