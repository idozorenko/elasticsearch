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
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.AggregatorFactories.*;
import org.elasticsearch.search.aggregations.InternalAggregation.*;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.*;

import java.io.*;
import java.util.*;

public final class ExactCardinalityAggregationBuilder
    extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, ExactCardinalityAggregationBuilder> {

    public static final String NAME = "exact_cardinality";
    private static final Type TYPE = new Type(NAME);


    public ExactCardinalityAggregationBuilder(String name, ValueType targetValueType) {
        super(name, TYPE, ValuesSourceType.ANY, targetValueType);
    }

    /**
     * Read from a stream.
     */
    public ExactCardinalityAggregationBuilder(StreamInput in) throws IOException {
        super(in, TYPE, ValuesSourceType.ANY);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected boolean serializeTargetValueType() {
        return true;
    }

    @Override
    protected ExactCardinalityAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource> config,
                                                           AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new ExactCardinalityAggregatorFactory(name, type, config, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return 0;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
