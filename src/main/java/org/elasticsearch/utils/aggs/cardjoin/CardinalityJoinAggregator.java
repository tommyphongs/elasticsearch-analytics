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

import org.elasticsearch.utils.common.LongArrayValueSource;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;


public class CardinalityJoinAggregator extends NumericMetricsAggregator.SingleValue {

    static final ParseField FILTERS_FIELD = new ParseField("queries");
    private static final Logger LOGGER = LogManager.getLogger(CardinalityJoinAggregator.class);
    private final String usingField;
    private final Weight[] weights;
    private final String[] names;
    private final UpdateSketch[] updateSketches;
    private final int[] counts;

    public CardinalityJoinAggregator(String name, SearchContext context, Aggregator parent,
                                     Map<String, Object> metaData, String field, int precisionThreshold,
                                     Weight[] weights, String[] names) throws IOException {
        super(name, context, parent, metaData);
        this.weights = weights;
        this.usingField = field;
        this.updateSketches = new UpdateSketch[weights.length];
        for (int i = 0; i < weights.length; i++) {
            this.updateSketches[i] = Sketches.updateSketchBuilder().setNominalEntries(
                    precisionThreshold).build();
        }
        this.names = names;
        this.counts = new int[names.length];
    }

    @Override
    public double metric(long owningBucketOrd) {
        return 0;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final Bits[] bits = new Bits[weights.length];
        for (int i = 0; i < weights.length; ++i) {
            bits[i] = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), weights[i].scorerSupplier(ctx));
        }
        LongArrayValueSource longArrayValueSource = new LongArrayValueSource(ctx, usingField);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                long[] value = longArrayValueSource.getLongs(doc);
                if (value != null) {
                    for (int i = 0; i < bits.length; i++) {
                        if (bits[i].get(doc)) {
                            for (long l : value) {
                                updateSketches[i].update(l);
                            }
                            counts[i]++;
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalCardinalityJoin(name, metadata(), updateSketches, names, counts);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinalityJoin(name, metadata(), new UpdateSketch[0], new String[0], new int[0]);
    }

}
