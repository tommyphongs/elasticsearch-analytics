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

import org.elasticsearch.utils.common.QueryBuilderParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CardinalityJoinAggregatorFactory extends AggregatorFactory {

    private static final Logger LOGGER = LogManager.getLogger(CardinalityJoinAggregatorFactory.class);
    private final String field;
    private final int precisionThreshold;
    private final Weight[] weights;
    private final String[] names;

    public CardinalityJoinAggregatorFactory(String name, List<QueryBuilderParser> filters,
                                            QueryShardContext queryShardContext, AggregatorFactory parent,
                                            AggregatorFactories.Builder subFactories,
                                            Map<String, Object> metaData, String field,
                                            int precisionThreshold) throws IOException {
        super(name, queryShardContext, parent, subFactories, metaData);
        this.field = field;
        this.precisionThreshold = precisionThreshold;
        weights = new Weight[filters.size()];
        names = new String[filters.size()];
        for (int i = 0; i < filters.size(); ++i) {
            QueryBuilderParser keyedFilter = filters.get(i);
            Query filter = keyedFilter.getQueryBuilder().toQuery(queryShardContext);
            this.weights[i] = queryShardContext.searcher().createWeight(
                    queryShardContext.searcher().rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
            this.names[i] = keyedFilter.key();
        }
    }


    @Override
    protected Aggregator createInternal(
            SearchContext searchContext, Aggregator aggregator,
            CardinalityUpperBound cardinalityUpperBound, Map<String, Object> map) throws IOException {
        return new CardinalityJoinAggregator(name, searchContext, aggregator, metadata, field, precisionThreshold, weights, names);
    }

}
