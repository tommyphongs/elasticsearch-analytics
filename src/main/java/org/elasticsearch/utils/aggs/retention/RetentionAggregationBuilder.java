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

package org.elasticsearch.utils.aggs.retention;

import org.elasticsearch.utils.common.AccuracyLevel;
import org.elasticsearch.utils.common.Constants;
import org.elasticsearch.utils.common.QueryBuilderParser;
import com.google.common.base.Preconditions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.utils.common.Utils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Map.Entry;

public class RetentionAggregationBuilder extends AbstractAggregationBuilder<RetentionAggregationBuilder> {
    public static final String NAME = "retention";

    private static final String COHORT_QUERY_QUERY_FIELD = "query";
    private static final ParseField TIME_FIELD = new ParseField("time_field");
    private static final ParseField KEY_FIELD = new ParseField("key_field");
    private static final ParseField START_DATE = new ParseField("start_date");
    private static final ParseField END_DATE = new ParseField("end_date");
    private static final ParseField DATE_INTERVAL = new ParseField("date_interval");
    private static final ParseField ACCURACY_LEVEL_FIELD = new ParseField("accuracy_level");
    private static final ParseField PRECISION_THRESHOLD = new ParseField("precision_threshold");
    private static final ParseField COHORT_QUERY_FIELD = new ParseField("cohort_query");
    private static final ObjectParser<RetentionAggregationBuilder, Void> PARSER = new ObjectParser<>(
            RetentionAggregationBuilder.NAME);

    static {
        PARSER.declareString(RetentionAggregationBuilder::setTimeField, TIME_FIELD);
        PARSER.declareString(RetentionAggregationBuilder::setIntervalField, DATE_INTERVAL);
        PARSER.declareString(RetentionAggregationBuilder::setKeyField, KEY_FIELD);
        PARSER.declareString(RetentionAggregationBuilder::setStartDate, START_DATE);
        PARSER.declareString(RetentionAggregationBuilder::setEndDate, END_DATE);
        PARSER.declareString(RetentionAggregationBuilder::setAccuracyField, ACCURACY_LEVEL_FIELD);
        PARSER.declareInt(RetentionAggregationBuilder::setPrecisionThreshold, PRECISION_THRESHOLD);
        PARSER.declareNamedObjects(RetentionAggregationBuilder::setQueryField, QueryBuilderParser.PARSER, COHORT_QUERY_FIELD);
    }

    private String timeField;
    private String keyField;
    private Integer interval;
    private Long startDateTimestamp;
    private Long endDateTimestamp;
    private Integer precisionThreshold;
    private QueryBuilder cohortQueryBuilder;
    private Map<String, QueryBuilder> comparedQueryBuilders = new TreeMap<>();

    /**
     * @param name the name of this aggregation
     */
    protected RetentionAggregationBuilder(String name) {
        super(name);
    }

    protected RetentionAggregationBuilder(RetentionAggregationBuilder clone,
                                          Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.cohortQueryBuilder = clone.cohortQueryBuilder;
        this.interval = clone.interval;
        this.precisionThreshold = clone.precisionThreshold;
        this.keyField = clone.keyField;
        this.startDateTimestamp = clone.startDateTimestamp;
        this.endDateTimestamp = clone.endDateTimestamp;
        this.comparedQueryBuilders = clone.comparedQueryBuilders;
    }

    /**
     * Read from a stream.
     */
    public RetentionAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.timeField = in.readString();
        this.keyField = in.readString();
        this.interval = in.readInt();
        this.startDateTimestamp = in.readLong();
        this.endDateTimestamp = in.readLong();
        this.precisionThreshold = in.readInt();
        this.cohortQueryBuilder = in.readNamedWriteable(QueryBuilder.class);
        this.comparedQueryBuilders = Utils.readString2QbMap(in);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        RetentionAggregationBuilder result = PARSER.parse(parser, new RetentionAggregationBuilder(aggregationName), null);
        result.checkConsistency();
        return result;
    }

    protected void checkConsistency() {
        Preconditions.checkArgument(timeField != null, "Missing time field");
        Preconditions.checkArgument(keyField != null, "Missing key field");
        Preconditions.checkArgument(interval != null, "Missing interval field");
        Preconditions.checkArgument(startDateTimestamp != null, "Missing from field");
        Preconditions.checkArgument(endDateTimestamp != null, "Missing to field");
        Preconditions.checkArgument(cohortQueryBuilder != null, "Missing cohort_query.query field");
        if (precisionThreshold == null) {
            this.precisionThreshold = AccuracyLevel.MEDIUM.getNominalEntries();
        }
        Preconditions.checkArgument(precisionThreshold > 0 && precisionThreshold < 1_000_000, "Precision " +
                "threshold must in range >0 and < 1000000 ");
    }

    protected void setQueryField(List<QueryBuilderParser> keyedQueries) {
        for (QueryBuilderParser queryBuilderParser : keyedQueries) {
            if (queryBuilderParser.key().equalsIgnoreCase(COHORT_QUERY_QUERY_FIELD)) {
                this.cohortQueryBuilder = queryBuilderParser.getQueryBuilder();
            } else {
                comparedQueryBuilders
                        .put(queryBuilderParser.key(), queryBuilderParser.getQueryBuilder());
            }
        }
        // internally we want to have a fixed order of filters, regardless of
        // the order of the filters in the request
    }

    public void setAccuracyField(String accuracyField) {
        if (this.precisionThreshold == null) {
            this.precisionThreshold = AccuracyLevel.getFromName(accuracyField).getNominalEntries();
        }
    }

    protected void addComparedQuery() {

    }

    public void setPrecisionThreshold(int precisionThreshold) {
        this.precisionThreshold = precisionThreshold;
    }

    public void setIntervalField(String interval) {
        if (interval == null) {
            throw new IllegalArgumentException("[interval field] must not be null: [" + name + "]");
        }
        this.interval = Integer.parseInt(interval);
        if (this.interval <= 0) {
            throw new IllegalArgumentException("Interval must larger zero");
        }
    }

    public void setKeyField(String keyField) {
        if (keyField == null) {
            throw new IllegalArgumentException("[key field] must not be null: [" + name + "]");
        }
        this.keyField = keyField;
    }

    public void setTimeField(String timeField) {
        if (timeField == null) {
            throw new IllegalArgumentException("[time field] must not be null: [" + name + "]");
        }
        this.timeField = timeField;
    }

    public void setStartDate(String fromFiled) {
        this.startDateTimestamp = LocalDate.parse(fromFiled).atStartOfDay().atOffset(ZoneOffset.UTC).toEpochSecond() * 1000;
    }

    public void setEndDate(String toFiled) {
        this.endDateTimestamp = LocalDate.parse(toFiled).atStartOfDay().atOffset(ZoneOffset.UTC).toEpochSecond() * 1000;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new RetentionAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(timeField);
        out.writeString(keyField);
        out.writeInt(interval);
        out.writeLong(startDateTimestamp);
        out.writeLong(endDateTimestamp);
        out.writeInt(precisionThreshold);
        out.writeNamedWriteable(cohortQueryBuilder);
        Utils.writeString2QbMap(out, this.comparedQueryBuilders);
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext context, AggregatorFactory parent, Builder subFactoriesBuilder)
            throws IOException {
        return new RetentionAggregatorFactory(name, cohortQueryBuilder, comparedQueryBuilders, context, parent,
                subFactoriesBuilder, metadata, keyField, timeField, interval, startDateTimestamp, endDateTimestamp, precisionThreshold);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TIME_FIELD.getPreferredName(), timeField)
                .field(KEY_FIELD.getPreferredName(), keyField)
                .field(START_DATE.getPreferredName(), Constants.DATE_FORMAT.format(new Date(startDateTimestamp)))
                .field(END_DATE.getPreferredName(), Constants.DATE_FORMAT.format(new Date(endDateTimestamp)))
                .field(DATE_INTERVAL.getPreferredName(), interval)
                .field(PRECISION_THRESHOLD.getPreferredName(), precisionThreshold);
        builder.startObject(COHORT_QUERY_FIELD.getPreferredName())
                .field(COHORT_QUERY_QUERY_FIELD, cohortQueryBuilder);
        for (Entry<String, QueryBuilder> e : comparedQueryBuilders.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timeField, startDateTimestamp, endDateTimestamp,
                cohortQueryBuilder, comparedQueryBuilders, interval, precisionThreshold);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        RetentionAggregationBuilder other = (RetentionAggregationBuilder) obj;
        return Objects.equals(cohortQueryBuilder.hashCode(), other.cohortQueryBuilder.hashCode())
                && Objects.hash(comparedQueryBuilders) == Objects.hash(other.comparedQueryBuilders);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
