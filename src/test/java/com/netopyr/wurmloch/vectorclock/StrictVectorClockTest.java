package com.netopyr.wurmloch.StrictVectorClock;

import com.netopyr.wurmloch.vectorclock.StrictVectorClock;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class StrictVectorClockTest {

    public static final String ID_1 = "ID_1";
    public static final String ID_2 = "ID_2";

    @Test
    public void shouldBeEqualToItself() {
        // given
        final StrictVectorClock vectorClock0 = new StrictVectorClock(ID_1);

        // then
        assertThat(vectorClock0.compareTo(vectorClock0), is(0));

        // when
        final StrictVectorClock vectorClock1 = vectorClock0.increment();

        // then
        assertThat(vectorClock1.compareTo(vectorClock1), is(0));

        // when
        final StrictVectorClock vectorClock11 = vectorClock1.increment();

        // then
        assertThat(vectorClock11.compareTo(vectorClock11), is(0));
    }

    @Test
    public void shouldBeEqualToItselfMerged() {
        // given
        final StrictVectorClock vectorClock0 = new StrictVectorClock(ID_1);

        // when
        final StrictVectorClock mergedClock0 = vectorClock0.merge(vectorClock0);

        // then
        assertThat(vectorClock0.compareTo(mergedClock0), is(0));
        assertThat(mergedClock0.compareTo(vectorClock0), is(0));

        // when
        final StrictVectorClock vectorClock1 = vectorClock0.increment();
        final StrictVectorClock mergedClock1 = vectorClock1.merge(vectorClock1);

        // then
        assertThat(vectorClock1.compareTo(mergedClock1), is(0));
        assertThat(mergedClock1.compareTo(vectorClock1), is(0));

        // when
        final StrictVectorClock vectorClock11 = vectorClock1.increment();
        final StrictVectorClock mergedClock11 = vectorClock11.merge(vectorClock11);

        // then
        assertThat(vectorClock11.compareTo(mergedClock11), is(0));
        assertThat(mergedClock11.compareTo(vectorClock11), is(0));
    }

    @Test
    public void newVectorClocksShouldBeEqual() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);
        final StrictVectorClock vectorClock2 = new StrictVectorClock(ID_2);

        // then
        assertThat(vectorClock1.compareTo(vectorClock2), is (lessThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock1), is (greaterThan(0)));
    }

    @Test
    public void newVectorClocksShouldBeEqualToThemselvesMerged() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);

        // when
        final StrictVectorClock mergedClock = vectorClock1.merge(vectorClock1);

        // then
        assertThat(vectorClock1.compareTo(mergedClock), is(0));
        assertThat(mergedClock.compareTo(vectorClock1), is(0));
    }

    @Test
    public void shouldBeGreaterWhenIncremented() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);

        // when
        final StrictVectorClock vectorClock2 = vectorClock1.increment();
        final StrictVectorClock vectorClock3 = vectorClock2.increment();

        // then
        assertThat(vectorClock1.compareTo(vectorClock1), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(vectorClock2), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock3), is(lessThan(0)));

        assertThat(vectorClock2.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock2), is(equalTo(0)));
        assertThat(vectorClock2.compareTo(vectorClock3), is(lessThan(0)));

        assertThat(vectorClock3.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock3.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(vectorClock3.compareTo(vectorClock3), is(equalTo(0)));
    }

    @Test
    public void shouldBeEqualToGreaterWhenMerged() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);
        final StrictVectorClock vectorClock2 = vectorClock1.increment();
        final StrictVectorClock vectorClock3 = vectorClock2.increment();

        // when
        final StrictVectorClock mergedClock1_2 = vectorClock1.merge(vectorClock2);
        final StrictVectorClock mergedClock1_3 = vectorClock1.merge(vectorClock3);
        final StrictVectorClock mergedClock2_1 = vectorClock2.merge(vectorClock1);
        final StrictVectorClock mergedClock2_3 = vectorClock2.merge(vectorClock3);
        final StrictVectorClock mergedClock3_1 = vectorClock3.merge(vectorClock1);
        final StrictVectorClock mergedClock3_2 = vectorClock3.merge(vectorClock2);

        // then
        assertThat(mergedClock1_2.compareTo(mergedClock1_2), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock1_2), is(equalTo(0)));
        assertThat(vectorClock3.compareTo(mergedClock1_2), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock2), is(equalTo(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock3), is(lessThan(0)));

        assertThat(mergedClock1_3.compareTo(mergedClock1_3), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(mergedClock1_3), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock1_3), is(lessThan(0)));
        assertThat(vectorClock3.compareTo(mergedClock1_3), is(equalTo(0)));
        assertThat(mergedClock1_3.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock1_3.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(mergedClock1_3.compareTo(vectorClock3), is(equalTo(0)));

        assertThat(mergedClock2_1.compareTo(mergedClock2_1), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(mergedClock2_1), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock2_1), is(equalTo(0)));
        assertThat(vectorClock3.compareTo(mergedClock2_1), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock2), is(equalTo(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock3), is(lessThan(0)));

        assertThat(mergedClock2_3.compareTo(mergedClock2_3), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(mergedClock2_3), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock2_3), is(lessThan(0)));
        assertThat(vectorClock3.compareTo(mergedClock2_3), is(equalTo(0)));
        assertThat(mergedClock2_3.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock2_3.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(mergedClock2_3.compareTo(vectorClock3), is(equalTo(0)));

        assertThat(mergedClock3_1.compareTo(mergedClock3_1), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(mergedClock3_1), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock3_1), is(lessThan(0)));
        assertThat(vectorClock3.compareTo(mergedClock3_1), is(equalTo(0)));
        assertThat(mergedClock3_1.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock3_1.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(mergedClock3_1.compareTo(vectorClock3), is(equalTo(0)));

        assertThat(mergedClock3_2.compareTo(mergedClock3_2), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(mergedClock3_2), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock3_2), is(lessThan(0)));
        assertThat(vectorClock3.compareTo(mergedClock3_2), is(equalTo(0)));
        assertThat(mergedClock3_2.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock3_2.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(mergedClock3_2.compareTo(vectorClock3), is(equalTo(0)));
    }

    @Test
    public void shouldBeEqualWhenIncrementedAtDifferentNodes() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);
        final StrictVectorClock vectorClock2 = new StrictVectorClock(ID_2);

        // when
        final StrictVectorClock vectorClock11 = vectorClock1.increment();
        final StrictVectorClock vectorClock111 = vectorClock11.increment();
        final StrictVectorClock vectorClock22 = vectorClock2.increment();
        final StrictVectorClock vectorClock222 = vectorClock22.increment();

        // then
        assertThat(vectorClock1.compareTo(vectorClock2), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock11), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock111), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock22), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock222), is(lessThan(0)));

        assertThat(vectorClock11.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock11.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(vectorClock11.compareTo(vectorClock111), is(lessThan(0)));
        assertThat(vectorClock11.compareTo(vectorClock22), is(lessThan(0)));
        assertThat(vectorClock11.compareTo(vectorClock222), is(lessThan(0)));

        assertThat(vectorClock111.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock111.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(vectorClock111.compareTo(vectorClock11), is(greaterThan(0)));
        assertThat(vectorClock111.compareTo(vectorClock22), is(lessThan(0)));
        assertThat(vectorClock111.compareTo(vectorClock222), is(lessThan(0)));

        assertThat(vectorClock2.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock11), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock111), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock22), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock222), is(lessThan(0)));

        assertThat(vectorClock22.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock22.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(vectorClock22.compareTo(vectorClock11), is(greaterThan(0)));
        assertThat(vectorClock22.compareTo(vectorClock111), is(greaterThan(0)));
        assertThat(vectorClock22.compareTo(vectorClock222), is(lessThan(0)));

        assertThat(vectorClock222.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock222.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(vectorClock222.compareTo(vectorClock11), is(greaterThan(0)));
        assertThat(vectorClock222.compareTo(vectorClock111), is(greaterThan(0)));
        assertThat(vectorClock222.compareTo(vectorClock22), is(greaterThan(0)));
    }

    @Test
    public void shouldBeGreaterWhenIncrementedAtDifferentNodesAndMerged() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);
        final StrictVectorClock vectorClock11 = vectorClock1.increment();
        final StrictVectorClock vectorClock2 = new StrictVectorClock(ID_2);
        final StrictVectorClock vectorClock22 = vectorClock2.increment();

        // when
        final StrictVectorClock mergedClock1_2 = vectorClock11.merge(vectorClock22);
        final StrictVectorClock mergedClock2_1 = vectorClock22.merge(vectorClock11);

        // then
        assertThat(vectorClock1.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(vectorClock2.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(vectorClock11.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock11.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(vectorClock22.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock22.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(mergedClock1_2.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock11), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock22), is(greaterThan(0)));

        assertThat(mergedClock2_1.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock2), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock11), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock22), is(greaterThan(0)));
    }

    @Test
    public void shouldBeGreaterWhenMergedAndIncrementedAtDifferentNodes() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);
        final StrictVectorClock vectorClock11 = vectorClock1.increment();
        final StrictVectorClock vectorClock2 = new StrictVectorClock(ID_2);
        final StrictVectorClock vectorClock22 = vectorClock2.increment();
        final StrictVectorClock mergedClock11_22 = vectorClock11.merge(vectorClock22);
        final StrictVectorClock mergedClock22_11 = vectorClock22.merge(vectorClock11);

        // when
        final StrictVectorClock clock11_22_1 = mergedClock11_22.increment();
        final StrictVectorClock clock22_11_2 = mergedClock22_11.increment();

        // then
        assertThat(mergedClock11_22.compareTo(clock11_22_1), is(lessThan(0)));
        assertThat(mergedClock11_22.compareTo(clock22_11_2), is(lessThan(0)));

        assertThat(clock11_22_1.compareTo(mergedClock11_22), is(greaterThan(0)));
        assertThat(clock22_11_2.compareTo(mergedClock11_22), is(greaterThan(0)));

        assertThat(mergedClock22_11.compareTo(clock11_22_1), is(lessThan(0)));
        assertThat(mergedClock22_11.compareTo(clock22_11_2), is(lessThan(0)));

        assertThat(clock11_22_1.compareTo(mergedClock22_11), is(greaterThan(0)));
        assertThat(clock22_11_2.compareTo(mergedClock22_11), is(greaterThan(0)));

        assertThat(clock11_22_1.compareTo(clock11_22_1), is(equalTo(0)));
        assertThat(clock11_22_1.compareTo(clock22_11_2), is(lessThan(0)));

        assertThat(clock22_11_2.compareTo(clock11_22_1), is(greaterThan(0)));
        assertThat(clock22_11_2.compareTo(clock22_11_2), is(equalTo(0)));
    }

    @Test
    public void shouldCalculateIdentical() {
        // given
        final StrictVectorClock vectorClock1 = new StrictVectorClock(ID_1);
        final StrictVectorClock vectorClock11 = vectorClock1.increment();
        final StrictVectorClock vectorClock111 = vectorClock11.increment();
        final StrictVectorClock vectorClock2 = new StrictVectorClock(ID_2);
        final StrictVectorClock vectorClock22 = vectorClock2.increment();
        final StrictVectorClock vectorClock11_22 = vectorClock11.merge(vectorClock22);
        final StrictVectorClock vectorClock22_11 = vectorClock22.merge(vectorClock11);
        final StrictVectorClock vectorClock11_22_1 = vectorClock11_22.increment();

        // then
        assertThat(vectorClock1.equals(vectorClock1), is(true));
        assertThat(vectorClock11.equals(vectorClock11), is(true));
        assertThat(vectorClock111.equals(vectorClock111), is(true));
        assertThat(vectorClock11_22.equals(vectorClock11_22), is(true));
        assertThat(vectorClock11_22_1.equals(vectorClock11_22_1), is(true));

        assertThat(vectorClock1.equals(vectorClock11), is(false));
        assertThat(vectorClock11.equals(vectorClock1), is(false));
        assertThat(vectorClock11.equals(vectorClock111), is(false));
        assertThat(vectorClock111.equals(vectorClock11), is(false));
        assertThat(vectorClock11.equals(vectorClock22), is(false));
        assertThat(vectorClock22.equals(vectorClock11), is(false));

        assertThat(vectorClock11.equals(vectorClock11_22), is(false));
        assertThat(vectorClock11_22.equals(vectorClock11), is(false));
        assertThat(vectorClock11.equals(vectorClock22_11), is(false));
        assertThat(vectorClock22_11.equals(vectorClock11), is(false));
        assertThat(vectorClock11_22.equals(vectorClock11_22_1), is(false));
        assertThat(vectorClock11_22_1.equals(vectorClock11_22), is(false));

        assertThat(vectorClock11_22.equals(vectorClock22_11), is(true));
        assertThat(vectorClock22_11.equals(vectorClock11_22), is(true));
    }

}
