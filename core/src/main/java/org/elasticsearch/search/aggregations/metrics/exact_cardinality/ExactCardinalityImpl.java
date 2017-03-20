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

import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.lease.*;
import org.elasticsearch.common.util.*;

import java.io.*;
import java.util.*;

/**
 * TODO
 * this is naive implementation, should refactor to memory-efficient
 */
public final class ExactCardinalityImpl implements Releasable {

    private static final float MAX_LOAD_FACTOR = 0.75f;


    private Map<Long, Set<Long>> m;

    public ExactCardinalityImpl(BigArrays bigArrays, long initialBucketCount) {
        m = new HashMap<>((int) initialBucketCount, MAX_LOAD_FACTOR);
    }

    public long maxBucket() {
        return m.keySet().size();
    }

    public void merge(long thisBucket, ExactCardinalityImpl other, long otherBucket) {
        getOrDefault(this, thisBucket).addAll(getOrDefault(other, otherBucket));
    }

    public void collect(long bucket, long hash) {
        Set<Long> set = getOrDefault(this, bucket);
        set.add(hash);
    }

    public long cardinality(long bucket) {
        return m.get(bucket).size();
    }

    @Override
    public void close() {

    }

    public void writeTo(long bucket, StreamOutput out) throws IOException {
        Set<Long> set = m.get(bucket);
        out.writeVLong(set.size());
        for(Long o : set){
            out.writeLong(o);
        }
    }

    private static Set<Long> getOrDefault(ExactCardinalityImpl counts, long bucketOrd){
        Set<Long> set = counts.m.get(bucketOrd);
        if(set == null){
            set = new HashSet<>();
            counts.m.put(bucketOrd, set);
        }
        return set;
    }

    public static ExactCardinalityImpl readFrom(StreamInput in, BigArrays bigArrays) throws IOException {
        ExactCardinalityImpl counts = new ExactCardinalityImpl(bigArrays, 1);
        Set<Long> set = getOrDefault(counts, 0);
        final long hsize = in.readVLong();
        for(long j = 0; j < hsize; ++j){
            final long encoded = in.readLong();
            set.add(encoded);
        }
        return counts;
    }

}
