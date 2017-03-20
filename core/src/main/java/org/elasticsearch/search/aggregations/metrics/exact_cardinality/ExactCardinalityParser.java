package org.elasticsearch.search.aggregations.metrics.exact_cardinality;

import org.elasticsearch.common.*;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.AnyValuesSourceParser;
import org.elasticsearch.search.aggregations.support.*;

import java.io.*;
import java.util.*;

public class ExactCardinalityParser extends AnyValuesSourceParser {

    public ExactCardinalityParser() {
        super(true, false);
    }

    @Override
    protected ExactCardinalityAggregationBuilder createFactory(String aggregationName, ValuesSourceType valuesSourceType,
                                                          ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        return new ExactCardinalityAggregationBuilder(aggregationName, targetValueType);
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token,
                            XContentParseContext context, Map<ParseField, Object> otherOptions) throws IOException {
        return false;
    }

}
