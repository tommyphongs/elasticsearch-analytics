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

import org.elasticsearch.utils.common.Utils;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class InternalCardinalityJoin extends InternalNumericMetricsAggregation.SingleValue implements CardinalityJoin {

    private static final Logger LOGGER = LogManager.getLogger(InternalCardinalityJoin.class);

    private static final String FIELD_INTERSECTION = "intersection";
    private static final String FIELD_UNION = "union";
    private static final String FIELD_COHORTS = "cohorts";
    private static final String FIELD_DOC_COUNTS = "doc_counts";

    private Sketch[] sketches;
    private String[] names;
    private int[] counts;

    protected InternalCardinalityJoin(String name, Map<String, Object> metadata,
                                      Sketch[] sketches, String[] names, int[] counts) {
        super(name, metadata);
        this.sketches = sketches;
        this.names = names;
        this.counts = counts;
    }

    public InternalCardinalityJoin(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        this.sketches = new Sketch[size];
        this.names = new String[size];
        this.counts = new int[size];
        IOException exception = Utils.doPrivileged(() -> {
            try {
                for (int i = 0; i < size; i++) {
                    Sketch sketch = Sketches.wrapSketch(Memory.wrap(in.readByteArray()));
                    sketches[i] = sketch;
                    names[i] = in.readString();
                    counts[i] = in.readVInt();
                }
            } catch (IOException e) {
                return e;
            }
            return null;
        });
        if (exception != null) {
            throw exception;
        }

    }

    @Override
    public double getValue() {
        Intersection intersection = SetOperation.builder().buildIntersection();
        for (Sketch sketch : sketches) {
            intersection.update(sketch);
        }
        return intersection.getResult().getEstimate();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        IOException e = Utils.doPrivileged(() -> {
            try {
                out.writeVInt(sketches.length);
                for (int i = 0; i < sketches.length; i++) {
                    out.writeByteArray(sketches[i].compact().toByteArray());
                    out.writeString(names[i]);
                    out.writeVInt(counts[i]);
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
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Union[] unions = new Union[sketches.length];
        for (int i = 0; i < sketches.length; i++) {
            Union u = SetOperation.builder().buildUnion();
            u.update(sketches[i]);
            unions[i] = u;
        }
        for (InternalAggregation aggregation : aggregations) {
            InternalCardinalityJoin internal = (InternalCardinalityJoin) aggregation;
            for (int i = 0; i < unions.length; i++) {
                unions[i].update(internal.sketches[i]);
                this.counts[i] += internal.counts[i];
            }
        }
        this.sketches = (Sketch[]) Arrays.stream(unions).map((Function<Union, Sketch>) Union::getResult).toArray();
        return this;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        Intersection intersection = SetOperation.builder().buildIntersection();
        Union union = SetOperation.builder().buildUnion();
        for (Sketch sketch : sketches) {
            intersection.update(sketch);
            union.update(sketch);
        }
        builder.field(FIELD_INTERSECTION, intersection.getResult().getEstimate());
        builder.field(FIELD_UNION, union.getResult().getEstimate());
        builder.startObject(FIELD_COHORTS);
        for (int i = 0; i < sketches.length; i++) {
            builder.field(names[i], sketches[i].getEstimate());
        }
        builder.endObject();
        builder.startObject(FIELD_DOC_COUNTS);
        for (int i = 0; i < sketches.length; i++) {
            builder.field(names[i], counts[i]);
        }
        builder.endObject();
        return builder;
    }


    @Override
    public String getWriteableName() {
        return CardinalityJoinAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }
}
