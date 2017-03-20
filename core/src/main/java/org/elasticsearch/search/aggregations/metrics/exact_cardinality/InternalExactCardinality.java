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
import org.elasticsearch.common.util.*;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.aggregations.pipeline.*;

import java.io.*;
import java.util.*;

public final class InternalExactCardinality extends InternalNumericMetricsAggregation.SingleValue implements
                                                                                                  ExactCardinality {
    private final ExactCardinalityImpl counts;

    InternalExactCardinality(String name, ExactCardinalityImpl counts, List<PipelineAggregator> pipelineAggregators,
                             Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.counts = counts;
    }

    /**
     * Read from a stream.
     */
    public InternalExactCardinality(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            counts = ExactCardinalityImpl.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            counts = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return ExactCardinalityAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalExactCardinality reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalExactCardinality cardinality = (InternalExactCardinality) aggregation;
            if (cardinality.counts != null) {
                if (reduced == null) {
                    reduced = new InternalExactCardinality(name, new ExactCardinalityImpl(BigArrays.NON_RECYCLING_INSTANCE, 1), pipelineAggregators(), getMetaData());
                }
                reduced.merge(cardinality);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalExactCardinality other) {
        assert counts != null && other != null;
        counts.merge(0, other.counts, 0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE, cardinality);
        return builder;
    }

}
