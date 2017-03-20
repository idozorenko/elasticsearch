package org.elasticsearch.search.aggregations.metrics.exact_cardinality;

import com.carrotsearch.hppc.*;
import org.elasticsearch.common.util.*;
import org.elasticsearch.test.*;

import static org.hamcrest.Matchers.equalTo;

public class ExactCardinalityImplTests extends ESTestCase {

    public void testAccuracy() {
        final long bucket = randomInt(20);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 100000);
        IntHashSet set = new IntHashSet();
        ExactCardinalityImpl e = new ExactCardinalityImpl(BigArrays.NON_RECYCLING_INSTANCE, 1);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            set.add(n);
            final long hash = BitMixer.mix64(n);
            e.collect(bucket, hash);
            if (randomInt(100) == 0) {
                //System.out.println(e.cardinality(bucket) + " <> " + set.size());
                assertThat((int) e.cardinality(bucket), equalTo(set.size()));
            }
        }
        assertThat((int) e.cardinality(bucket), equalTo(set.size()));
    }

    public void testMerge() {
        final ExactCardinalityImpl single = new ExactCardinalityImpl(BigArrays.NON_RECYCLING_INSTANCE, 0);
        final ExactCardinalityImpl[] multi = new ExactCardinalityImpl[randomIntBetween(2, 100)];
        final long[] bucketOrds = new long[multi.length];
        for (int i = 0; i < multi.length; ++i) {
            bucketOrds[i] = randomInt(20);
            multi[i] = new ExactCardinalityImpl(BigArrays.NON_RECYCLING_INSTANCE, 5);
        }
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(0, hash);
            // use a gaussian so that all instances don't collect as many hashes
            final int index = (int) (Math.pow(randomDouble(), 2));
            multi[index].collect(bucketOrds[index], hash);
            if (randomInt(100) == 0) {
                ExactCardinalityImpl merged = new ExactCardinalityImpl(BigArrays.NON_RECYCLING_INSTANCE, 0);
                for (int j = 0; j < multi.length; ++j) {
                    merged.merge(0, multi[j], bucketOrds[j]);
                }
                assertEquals(single.cardinality(0), merged.cardinality(0));
            }
        }
    }

}
