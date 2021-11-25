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

package org.elasticsearch.utils.aggs.cardjoin;

import org.elasticsearch.utils.common.AccuracyLevel;
import org.elasticsearch.utils.common.QueryBuilderParser;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.*;

public class CardinalityJoinAggregationBuilder extends AbstractAggregationBuilder<CardinalityJoinAggregationBuilder> {
    public static final String NAME = "cardinality_join";
    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField ACCURACY_LEVEL = new ParseField("accuracy_level");
    private static final ParseField PRECISION_THRESHOLD = new ParseField("precision_threshold");
    private static final ParseField QUERIES_FIELD = new ParseField("queries");
    private static final ObjectParser<CardinalityJoinAggregationBuilder, Void> PARSER = new ObjectParser<>(
            CardinalityJoinAggregationBuilder.NAME);

    static {
        PARSER.declareString(CardinalityJoinAggregationBuilder::setField, FIELD);
        PARSER.declareString(CardinalityJoinAggregationBuilder::setAccuracyField, ACCURACY_LEVEL);
        PARSER.declareInt(CardinalityJoinAggregationBuilder::setPrecisionThreshold, PRECISION_THRESHOLD);
        PARSER.declareNamedObjects(CardinalityJoinAggregationBuilder::setQueriesAsList, QueryBuilderParser.PARSER, QUERIES_FIELD);
    }

    private String field;
    private Integer precisionThreshold;
    private List<QueryBuilderParser> queries;

    /**
     * @param name the name of this aggregation
     */
    protected CardinalityJoinAggregationBuilder(String name) {
        super(name);
    }

    protected CardinalityJoinAggregationBuilder(CardinalityJoinAggregationBuilder clone,
                                                Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.queries = new ArrayList<>(clone.queries);
    }

    /**
     * Read from a stream.
     */
    public CardinalityJoinAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.precisionThreshold = in.readInt();
        int queriesSize = in.readVInt();
        this.queries = new ArrayList<>(queriesSize);
        for (int i = 0; i < queriesSize; i++) {
            queries.add(new QueryBuilderParser(in));
        }
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        CardinalityJoinAggregationBuilder result = PARSER.parse(parser, new CardinalityJoinAggregationBuilder(aggregationName), null);
        result.checkConsistency();
        return result;
    }

    protected void checkConsistency() {
        if ((queries == null) || (queries.size() == 0)) {
            throw new IllegalStateException("[" + name + "] is missing : " + QUERIES_FIELD.getPreferredName() + " parameter");
        }
        if (field == null) {
            throw new IllegalArgumentException("Missing using field");
        }
        if (precisionThreshold == null) {
            this.precisionThreshold = AccuracyLevel.MEDIUM.getNominalEntries();
        }

    }

    protected void setQueriesAsList(List<QueryBuilderParser> queries) {
        this.queries = new ArrayList<>(queries);
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
        this.queries.sort(Comparator.comparing(QueryBuilderParser::key));
    }

    public void setAccuracyField(String accuracyField) {
        if (precisionThreshold == null) {
            this.precisionThreshold = AccuracyLevel.getFromName(accuracyField).getNominalEntries();
        }
    }

    public void setPrecisionThreshold(int precisionThreshold) {
        this.precisionThreshold = precisionThreshold;
    }

    public void setField(String using) {
        if (using == null) {
            throw new IllegalArgumentException("[using] must not be null: [" + name + "]");
        }
        this.field = using;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new CardinalityJoinAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeInt(this.precisionThreshold);
        out.writeVInt(queries.size());
        for (QueryBuilderParser queryBuilderParser : queries) {
            queryBuilderParser.writeTo(out);
        }
    }

    /**
     * Get the filters. This will be an unmodifiable map
     */
    public Map<String, QueryBuilder> filters() {
        Map<String, QueryBuilder> result = new HashMap<>(this.queries.size());
        for (QueryBuilderParser queryBuilderParser : this.queries) {
            result.put(queryBuilderParser.key(), queryBuilderParser.getQueryBuilder());
        }
        return result;
    }


    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
            throws IOException {
        int maxFilters = queryShardContext.getIndexSettings().getMaxAdjacencyMatrixFilters();
        if (queries.size() > maxFilters) {
            throw new IllegalArgumentException(
                    "Number of filters is too large, must be less than or equal to: [" + maxFilters + "] but was ["
                            + queries.size() + "]."
                            + "This limit can be set by changing the [" + IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey()
                            + "] index level setting.");
        }

        List<QueryBuilderParser> rewrittenFilters = new ArrayList<>(queries.size());
        for (QueryBuilderParser kf : queries) {
            rewrittenFilters.add(new QueryBuilderParser(kf.key(), Rewriteable.rewrite(kf.getQueryBuilder(),
                    queryShardContext, true)));
        }

        return new CardinalityJoinAggregatorFactory(name, rewrittenFilters, queryShardContext, parent,
                subFactoriesBuilder, metadata, field, precisionThreshold);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("field", field);
        builder.field("precision_threshold", precisionThreshold);
        builder.startObject(CardinalityJoinAggregator.FILTERS_FIELD.getPreferredName());
        for (QueryBuilderParser queryBuilderParser : queries) {
            builder.field(queryBuilderParser.key(), queryBuilderParser.getQueryBuilder());
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queries, precisionThreshold, field);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        CardinalityJoinAggregationBuilder other = (CardinalityJoinAggregationBuilder) obj;
        return Objects.equals(queries, other.queries);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
