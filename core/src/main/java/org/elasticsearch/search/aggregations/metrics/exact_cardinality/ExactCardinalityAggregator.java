/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.exact_cardinality;

import com.carrotsearch.hppc.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.*;
import org.elasticsearch.common.*;
import org.elasticsearch.common.lease.*;
import org.elasticsearch.common.util.*;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.*;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.*;

import java.io.*;
import java.util.*;

/**
 * An aggregator that computes exact counts of unique values.
 */
@SuppressWarnings("Duplicates")
public class ExactCardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource valuesSource;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    private ExactCardinalityImpl counts;

    private Collector collector;

    public ExactCardinalityAggregator(String name, ValuesSource valuesSource,
                                      AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.counts = new ExactCardinalityImpl(context.bigArrays(), 1);
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return new EmptyCollector();
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            MurmurHash3Values hashValues = source.isFloatingPoint() ? MurmurHash3Values.hash(source.doubleValues(ctx)) : MurmurHash3Values
                .hash(source.longValues(ctx));
            return new DirectCollector(counts, hashValues);
        }

        return new DirectCollector(counts, MurmurHash3Values.hash(valuesSource.bytesValues(ctx)));
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();

        collector = pickCollector(ctx);
        return collector;
    }

    private void postCollectLastCollector() {
        if (collector != null) {
            try {
                collector.postCollect();
                collector.close();
            } finally {
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() {
        postCollectLastCollector();
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        ExactCardinalityImpl copy = new ExactCardinalityImpl(BigArrays.NON_RECYCLING_INSTANCE, 1);
        copy.merge(0, counts, owningBucketOrdinal);
        return new InternalExactCardinality(name, copy, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalExactCardinality(name, null, pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    private abstract static class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect();

    }

    private static class EmptyCollector extends Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static class DirectCollector extends Collector {

        private final MurmurHash3Values hashes;
        private final ExactCardinalityImpl counts;

        DirectCollector(ExactCardinalityImpl counts, MurmurHash3Values values) {
            this.counts = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            hashes.setDocument(doc);
            final int valueCount = hashes.count();
            for (int i = 0; i < valueCount; ++i) {
                counts.collect(bucketOrd, hashes.valueAt(i));
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }

    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     */
    abstract static class MurmurHash3Values {

        public abstract void setDocument(int docId);

        public abstract int count();

        public abstract long valueAt(int index);

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3Values hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3Values hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3Values hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3Values {

            private final SortedNumericDocValues values;

            public Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return BitMixer.mix64(values.valueAt(index));
            }
        }

        private static class Double extends MurmurHash3Values {

            private final SortedNumericDoubleValues values;

            public Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(values.valueAt(index)));
            }
        }

        private static class Bytes extends MurmurHash3Values {

            private final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            public Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                final BytesRef bytes = values.valueAt(index);
                org.elasticsearch.common.hash.MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
