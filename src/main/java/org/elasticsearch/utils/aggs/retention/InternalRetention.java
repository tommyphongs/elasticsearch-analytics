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

import org.elasticsearch.utils.common.Constants;
import org.elasticsearch.utils.common.Utils;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import org.apache.datasketches.theta.Sketch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.utils.aggs.retention.RetentionAggregator.AggregationSketch;
import static org.elasticsearch.utils.aggs.retention.RetentionAggregator.AggregationSketchUnion;

public class InternalRetention extends InternalMultiBucketAggregation implements Retention {

    private static final Logger LOGGER = LogManager.getLogger(InternalRetention.class);

    private static final String JSON_DELIMITER = "_";
    private static final String JSON_BASE = "base";
    private static final String JSON_COHORT = "cohort";
    private static final String JSON_COHORT_0 = "cohort0";
    private static final String JSON_AND_MIDFIX = JSON_DELIMITER + "AND" + JSON_DELIMITER;
    private static final String JSON_DATE = "date";

    private List<AggregationSketch> aggregationSketches;

    protected InternalRetention(String name, Map<String, Object> metaData,
                                List<? extends AggregationSketch> aggregationSketches) {
        super(name, metaData);
        this.aggregationSketches = aggregationSketches.stream().map(
                item -> (AggregationSketch) item).collect(Collectors.toList());
    }

    public InternalRetention(StreamInput in) throws IOException {
        super(in);
        this.aggregationSketches = new ArrayList<>();
        IOException e = Utils.doPrivileged(() -> {
            try {
                int length = in.readInt();
                for (int i = 0; i < length; i++) {
                    this.aggregationSketches.add(AggregationSketch.doReadFrom(in));
                }
            } catch (IOException e1) {
                return e1;
            }
            return null;
        });
        if (e != null) {
            throw e;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        IOException e = Utils.doPrivileged(() -> {
            try {
                out.writeInt(aggregationSketches.size());
                for (AggregationSketch aggregationSketch : this.aggregationSketches) {
                    aggregationSketch.doWriteTo(out);
                }
            } catch (IOException e1) {
                return e1;
            }
            return null;
        });
        if (e != null) {
            throw e;
        }

    }

    @Override
    public InternalMultiBucketAggregation create(List buckets) {
        return null;
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return null;
    }

    @Override
    protected InternalBucket reduceBucket(List buckets, ReduceContext context) {
        return null;
    }

    @Override
    public List<? extends InternalBucket> getBuckets() {
        return null;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<AggregationSketchUnion> unions = new ArrayList<>();
        for (int i = 0; i < aggregationSketches.size(); i++) {
            AggregationSketchUnion union = new AggregationSketchUnion(aggregationSketches.get(i).getTimestamp());
            unions.add(union);
            for (InternalAggregation aggregation : aggregations) {
                InternalRetention internal = (InternalRetention) aggregation;
                union.update(internal.aggregationSketches.get(i));
            }
        }
        this.aggregationSketches = unions.stream()
                .map(AggregationSketchUnion::toAggregationSketch)
                .collect(Collectors.toList());
        return this;
    }

    private void putIntersection(Map<String, Object> target,
                                 String name1, String name2,
                                 Sketch sketch1, Sketch sketch2) {
        String key = name1 + JSON_AND_MIDFIX + name2;
        target.put(key, Utils.estimateIntersection(sketch1, sketch2));
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        for (int i = 0; i < aggregationSketches.size(); i++) {
            AggregationSketch cohort0sketch = aggregationSketches.get(i);
            List<Map<String, Object>> list = new ArrayList<>();
            for (int k = i; k < aggregationSketches.size(); k++) {
                AggregationSketch currentCohort = aggregationSketches.get(k);
                Map<String, Object> map = new Object2ObjectArrayMap<>();
                map.put(JSON_DATE, Constants.DATE_FORMAT.format(new Date(currentCohort.getTimestamp())));
                map.put(JSON_BASE, currentCohort.getBase().getEstimate());
                putIntersection(map, JSON_BASE, JSON_COHORT_0, currentCohort.getBase(), cohort0sketch.getCohort());
                map.put(JSON_COHORT, currentCohort.getCohort().getEstimate());
                putIntersection(map, JSON_COHORT, JSON_COHORT_0, currentCohort.getCohort(), cohort0sketch.getCohort());
                currentCohort.getComparedCohorts().forEach((cohortName, sketch) -> {
                            map.put(cohortName, sketch.getEstimate());
                            putIntersection(map, cohortName, JSON_COHORT_0, sketch, cohort0sketch.getCohort());
                        }
                );
                list.add(map);
            }
            builder.array(Constants.DATE_FORMAT.format(new Date(cohort0sketch.getTimestamp())), list);
        }

        return builder;
    }

    @Override
    public String getWriteableName() {
        return RetentionAggregationBuilder.NAME;
    }

}
