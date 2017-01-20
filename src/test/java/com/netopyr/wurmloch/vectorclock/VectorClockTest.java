package com.netopyr.wurmloch.vectorclock;

import com.netopyr.wurmloch.vectorclock.VectorClock;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class VectorClockTest {

    public static final String ID_1 = "ID_1";
    public static final String ID_2 = "ID_2";

    @Test
    public void shouldBeEqualToItself() {
        // given
        final VectorClock vectorClock0 = new VectorClock();

        // then
        assertThat(vectorClock0.compareTo(vectorClock0), is(0));

        // when
        final VectorClock vectorClock1 = vectorClock0.increment(ID_1);

        // then
        assertThat(vectorClock1.compareTo(vectorClock1), is(0));

        // when
        final VectorClock vectorClock11 = vectorClock1.increment(ID_1);

        // then
        assertThat(vectorClock11.compareTo(vectorClock11), is(0));
    }

    @Test
    public void shouldBeEqualToItselfMerged() {
        // given
        final VectorClock vectorClock0 = new VectorClock();

        // when
        final VectorClock mergedClock0 = vectorClock0.merge(vectorClock0);

        // then
        assertThat(vectorClock0.compareTo(mergedClock0), is(0));
        assertThat(mergedClock0.compareTo(vectorClock0), is(0));

        // when
        final VectorClock vectorClock1 = vectorClock0.increment(ID_1);
        final VectorClock mergedClock1 = vectorClock1.merge(vectorClock1);

        // then
        assertThat(vectorClock1.compareTo(mergedClock1), is(0));
        assertThat(mergedClock1.compareTo(vectorClock1), is(0));

        // when
        final VectorClock vectorClock11 = vectorClock1.increment(ID_1);
        final VectorClock mergedClock11 = vectorClock11.merge(vectorClock11);

        // then
        assertThat(vectorClock11.compareTo(mergedClock11), is(0));
        assertThat(mergedClock11.compareTo(vectorClock11), is(0));
    }

    @Test
    public void newVectorClocksShouldBeEqual() {
        // given
        final VectorClock vectorClock1 = new VectorClock();
        final VectorClock vectorClock2 = new VectorClock();

        // then
        assertThat(vectorClock1.compareTo(vectorClock2), is (0));
        assertThat(vectorClock2.compareTo(vectorClock1), is (0));
    }

    @Test
    public void newVectorClocksShouldBeEqualToThemselvesMerged() {
        // given
        final VectorClock vectorClock1 = new VectorClock();
        final VectorClock vectorClock2 = new VectorClock();

        // when
        final VectorClock mergedClock = vectorClock1.merge(vectorClock2);

        // then
        assertThat(vectorClock1.compareTo(mergedClock), is(0));
        assertThat(mergedClock.compareTo(vectorClock1), is(0));

        assertThat(vectorClock2.compareTo(mergedClock), is(0));
        assertThat(mergedClock.compareTo(vectorClock2), is(0));
    }

    @Test
    public void shouldBeGreaterWhenIncremented() {
        // given
        final VectorClock vectorClock1 = new VectorClock();

        // when
        final VectorClock vectorClock2 = vectorClock1.increment(ID_1);
        final VectorClock vectorClock3 = vectorClock2.increment(ID_1);

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
        final VectorClock vectorClock1 = new VectorClock();
        final VectorClock vectorClock2 = vectorClock1.increment(ID_1);
        final VectorClock vectorClock3 = vectorClock2.increment(ID_1);

        // when
        final VectorClock mergedClock1_2 = vectorClock1.merge(vectorClock2);
        final VectorClock mergedClock1_3 = vectorClock1.merge(vectorClock3);
        final VectorClock mergedClock2_1 = vectorClock2.merge(vectorClock1);
        final VectorClock mergedClock2_3 = vectorClock2.merge(vectorClock3);
        final VectorClock mergedClock3_1 = vectorClock3.merge(vectorClock1);
        final VectorClock mergedClock3_2 = vectorClock3.merge(vectorClock2);

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
        final VectorClock vectorClock0 = new VectorClock();

        // when
        final VectorClock vectorClock1 = vectorClock0.increment(ID_1);
        final VectorClock vectorClock11 = vectorClock1.increment(ID_1);
        final VectorClock vectorClock2 = vectorClock0.increment(ID_2);
        final VectorClock vectorClock22 = vectorClock2.increment(ID_2);

        // then
        assertThat(vectorClock0.compareTo(vectorClock1), is(lessThan(0)));
        assertThat(vectorClock0.compareTo(vectorClock11), is(lessThan(0)));
        assertThat(vectorClock0.compareTo(vectorClock2), is(lessThan(0)));
        assertThat(vectorClock0.compareTo(vectorClock22), is(lessThan(0)));

        assertThat(vectorClock1.compareTo(vectorClock0), is(greaterThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock11), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(vectorClock2), is(equalTo(0)));
        assertThat(vectorClock1.compareTo(vectorClock22), is(equalTo(0)));

        assertThat(vectorClock11.compareTo(vectorClock0), is(greaterThan(0)));
        assertThat(vectorClock11.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(vectorClock11.compareTo(vectorClock2), is(equalTo(0)));
        assertThat(vectorClock11.compareTo(vectorClock22), is(equalTo(0)));

        assertThat(vectorClock2.compareTo(vectorClock0), is(greaterThan(0)));
        assertThat(vectorClock2.compareTo(vectorClock1), is(equalTo(0)));
        assertThat(vectorClock2.compareTo(vectorClock11), is(equalTo(0)));
        assertThat(vectorClock2.compareTo(vectorClock22), is(lessThan(0)));

        assertThat(vectorClock22.compareTo(vectorClock0), is(greaterThan(0)));
        assertThat(vectorClock22.compareTo(vectorClock1), is(equalTo(0)));
        assertThat(vectorClock22.compareTo(vectorClock11), is(equalTo(0)));
        assertThat(vectorClock22.compareTo(vectorClock2), is(greaterThan(0)));
    }

    @Test
    public void shouldBeGreaterWhenIncrementedAtDifferentNodesAndMerged() {
        // given
        final VectorClock vectorClock0 = new VectorClock();
        final VectorClock vectorClock1 = vectorClock0.increment(ID_1);
        final VectorClock vectorClock2 = vectorClock0.increment(ID_2);

        // when
        final VectorClock mergedClock1_2 = vectorClock1.merge(vectorClock2);
        final VectorClock mergedClock2_1 = vectorClock2.merge(vectorClock1);

        // then
        assertThat(vectorClock0.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock0.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(vectorClock1.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock1.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(vectorClock2.compareTo(mergedClock1_2), is(lessThan(0)));
        assertThat(vectorClock2.compareTo(mergedClock2_1), is(lessThan(0)));

        assertThat(mergedClock1_2.compareTo(vectorClock0), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock1_2.compareTo(vectorClock2), is(greaterThan(0)));

        assertThat(mergedClock2_1.compareTo(vectorClock0), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock1), is(greaterThan(0)));
        assertThat(mergedClock2_1.compareTo(vectorClock2), is(greaterThan(0)));
    }

    @Test
    public void shouldBeGreaterWhenMergedAndIncrementedAtDifferentNodes() {
        // given
        final VectorClock vectorClock0 = new VectorClock();
        final VectorClock vectorClock1 = vectorClock0.increment(ID_1);
        final VectorClock vectorClock2 = vectorClock0.increment(ID_2);
        final VectorClock mergedClock1_2 = vectorClock1.merge(vectorClock2);
        final VectorClock mergedClock2_1 = vectorClock2.merge(vectorClock1);

        // when
        final VectorClock clock1_2_1 = mergedClock1_2.increment(ID_1);
        final VectorClock clock1_2_2 = mergedClock1_2.increment(ID_2);
        final VectorClock clock2_1_1 = mergedClock2_1.increment(ID_1);
        final VectorClock clock2_1_2 = mergedClock2_1.increment(ID_2);

        // then
        assertThat(mergedClock1_2.compareTo(clock1_2_1), is(lessThan(0)));
        assertThat(mergedClock1_2.compareTo(clock1_2_2), is(lessThan(0)));
        assertThat(mergedClock1_2.compareTo(clock2_1_1), is(lessThan(0)));
        assertThat(mergedClock1_2.compareTo(clock2_1_2), is(lessThan(0)));

        assertThat(clock1_2_1.compareTo(mergedClock1_2), is(greaterThan(0)));
        assertThat(clock1_2_2.compareTo(mergedClock1_2), is(greaterThan(0)));
        assertThat(clock2_1_1.compareTo(mergedClock1_2), is(greaterThan(0)));
        assertThat(clock2_1_2.compareTo(mergedClock1_2), is(greaterThan(0)));

        assertThat(mergedClock2_1.compareTo(clock1_2_1), is(lessThan(0)));
        assertThat(mergedClock2_1.compareTo(clock1_2_2), is(lessThan(0)));
        assertThat(mergedClock2_1.compareTo(clock2_1_1), is(lessThan(0)));
        assertThat(mergedClock2_1.compareTo(clock2_1_2), is(lessThan(0)));

        assertThat(clock1_2_1.compareTo(mergedClock2_1), is(greaterThan(0)));
        assertThat(clock1_2_2.compareTo(mergedClock2_1), is(greaterThan(0)));
        assertThat(clock2_1_1.compareTo(mergedClock2_1), is(greaterThan(0)));
        assertThat(clock2_1_2.compareTo(mergedClock2_1), is(greaterThan(0)));

        assertThat(clock1_2_1.compareTo(clock1_2_1), is(equalTo(0)));
        assertThat(clock1_2_1.compareTo(clock1_2_2), is(equalTo(0)));
        assertThat(clock1_2_1.compareTo(clock2_1_1), is(equalTo(0)));
        assertThat(clock1_2_1.compareTo(clock2_1_2), is(equalTo(0)));

        assertThat(clock1_2_2.compareTo(clock1_2_1), is(equalTo(0)));
        assertThat(clock1_2_2.compareTo(clock1_2_2), is(equalTo(0)));
        assertThat(clock1_2_2.compareTo(clock2_1_1), is(equalTo(0)));
        assertThat(clock1_2_2.compareTo(clock2_1_2), is(equalTo(0)));

        assertThat(clock2_1_1.compareTo(clock1_2_1), is(equalTo(0)));
        assertThat(clock2_1_1.compareTo(clock1_2_2), is(equalTo(0)));
        assertThat(clock2_1_1.compareTo(clock2_1_1), is(equalTo(0)));
        assertThat(clock2_1_1.compareTo(clock2_1_2), is(equalTo(0)));

        assertThat(clock2_1_2.compareTo(clock1_2_1), is(equalTo(0)));
        assertThat(clock2_1_2.compareTo(clock1_2_2), is(equalTo(0)));
        assertThat(clock2_1_2.compareTo(clock2_1_1), is(equalTo(0)));
        assertThat(clock1_2_2.compareTo(clock2_1_2), is(equalTo(0)));
    }

    @Test
    public void shouldCalculateIdentical() {
        // given
        final VectorClock vectorClock0 = new VectorClock();
        final VectorClock vectorClock1 = vectorClock0.increment(ID_1);
        final VectorClock vectorClock11 = vectorClock1.increment(ID_1);
        final VectorClock vectorClock2 = vectorClock0.increment(ID_2);
        final VectorClock vectorClock1_2 = vectorClock1.merge(vectorClock2);
        final VectorClock vectorClock2_1 = vectorClock2.merge(vectorClock1);
        final VectorClock vectorClock1_2_1 = vectorClock1_2.increment(ID_1);

        // then
        assertThat(vectorClock0.equals(vectorClock0), is(true));
        assertThat(vectorClock1.equals(vectorClock1), is(true));
        assertThat(vectorClock11.equals(vectorClock11), is(true));
        assertThat(vectorClock1_2.equals(vectorClock1_2), is(true));
        assertThat(vectorClock1_2_1.equals(vectorClock1_2_1), is(true));

        assertThat(vectorClock0.equals(vectorClock1), is(false));
        assertThat(vectorClock1.equals(vectorClock0), is(false));
        assertThat(vectorClock1.equals(vectorClock11), is(false));
        assertThat(vectorClock11.equals(vectorClock1), is(false));
        assertThat(vectorClock1.equals(vectorClock2), is(false));
        assertThat(vectorClock2.equals(vectorClock1), is(false));

        assertThat(vectorClock1.equals(vectorClock1_2), is(false));
        assertThat(vectorClock1_2.equals(vectorClock1), is(false));
        assertThat(vectorClock1.equals(vectorClock2_1), is(false));
        assertThat(vectorClock2_1.equals(vectorClock1), is(false));
        assertThat(vectorClock1_2.equals(vectorClock1_2_1), is(false));
        assertThat(vectorClock1_2_1.equals(vectorClock1_2), is(false));

        assertThat(vectorClock1_2.equals(vectorClock2_1), is(true));
        assertThat(vectorClock2_1.equals(vectorClock1_2), is(true));
    }

}
