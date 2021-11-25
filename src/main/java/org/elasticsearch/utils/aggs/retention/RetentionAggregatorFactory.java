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

import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class RetentionAggregatorFactory extends AggregatorFactory {

    private static final Logger LOGGER = LogManager.getLogger(RetentionAggregatorFactory.class);
    private final String keyField;
    private final String timeField;
    private final int interval;
    private final long fromTimestamp;
    private final long toTimestamp;
    private final int precisionThreshold;
    private final Weight weight;
    private final Map<String, Weight> comparedCohortWeights;

    public RetentionAggregatorFactory(
            String name, QueryBuilder cohortQueryBuilder, Map<String, QueryBuilder> comparedQueryBuilders,
            QueryShardContext queryShardContext, AggregatorFactory parent, AggregatorFactories.Builder subFactories,
            Map<String, Object> metaData, String keyField, String timeField, int interval,
            long fromTimestamp, long toTimestamp,
            int precisionThreshold) throws IOException {
        super(name, queryShardContext, parent, subFactories, metaData);
        this.keyField = keyField;
        this.timeField = timeField;
        this.interval = interval;
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
        this.precisionThreshold = precisionThreshold;
        this.weight = search(queryShardContext, cohortQueryBuilder);
        this.comparedCohortWeights = new Object2ObjectAVLTreeMap<>();
        for (Map.Entry<String, QueryBuilder> entry : comparedQueryBuilders.entrySet()) {
            this.comparedCohortWeights.put(entry.getKey(), search(queryShardContext, entry.getValue()));
        }
    }

    private static Weight search(QueryShardContext queryShardContext, QueryBuilder queryBuilder) throws IOException {
        Query filter = queryBuilder.toQuery(queryShardContext);
        return queryShardContext.searcher().createWeight(
                queryShardContext.searcher().rewrite(filter),
                ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    @Override
    protected Aggregator createInternal(SearchContext searchContext, Aggregator aggregator,
                                        CardinalityUpperBound cardinalityUpperBound, Map<String, Object> map) throws IOException {
        return new RetentionAggregator(
                name, factories, searchContext, aggregator, cardinalityUpperBound, metadata, keyField,
                timeField, interval, fromTimestamp, toTimestamp, precisionThreshold, weight, comparedCohortWeights);
    }
}
