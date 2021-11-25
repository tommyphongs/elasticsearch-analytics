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

import org.elasticsearch.utils.common.LongArrayValueSource;
import org.elasticsearch.utils.common.LongValueSource;
import org.elasticsearch.utils.common.Utils;
import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;


public class RetentionAggregator extends BucketsAggregator {

    public static final long ONE_DAY_MILLISECOND = 86_400_000;
    private static final Logger LOGGER = LogManager.getLogger(RetentionAggregator.class);

    private final String keyField;
    private final String timeField;

    // interval by date
    private final int interval;
    private final long timestampInterval;
    private final long fromTimestamp;
    private final long toTimestamp;
    private final Weight cohortWeight;
    private final Map<String, Weight> comparedCohortWeights;
    private final ArrayList<UpdatableAggregationSketch> aggregationSketches;

    public RetentionAggregator(String name, AggregatorFactories aggregatorFactories, SearchContext context, Aggregator parentAgg,
                               CardinalityUpperBound bucketCardinality,
                               Map<String, Object> metaData, String keyField, String timeField, int interval,
                               long fromTimestamp, long toTimestamp,
                               int precisionThreshold, Weight cohortWeight,
                               Map<String, Weight> comparedCohortWeights) throws IOException {
        super(name, aggregatorFactories, context, parentAgg, bucketCardinality, metaData);

        this.fromTimestamp = fromTimestamp;
        this.toTimestamp = toTimestamp;
        int numberOfDay = (int) ((toTimestamp - fromTimestamp) / (ONE_DAY_MILLISECOND));
        this.aggregationSketches = new ArrayList<>();
        this.timeField = timeField;
        this.interval = interval;
        this.timestampInterval = interval * ONE_DAY_MILLISECOND;
        int nBucket = numberOfDay / interval;
        if (numberOfDay % interval > 0) {
            nBucket++;
        }
        for (int i = 0; i < nBucket; i++) {
            long timestamp = fromTimestamp + i * interval * ONE_DAY_MILLISECOND;
            UpdateSketch base = Sketches.updateSketchBuilder().setNominalEntries(precisionThreshold).build();
            UpdateSketch cohort = Sketches.updateSketchBuilder().setNominalEntries(precisionThreshold).build();
            Map<String, Sketch> comparedCohorts = new Object2ObjectAVLTreeMap<>();
            Map<String, UpdateSketch> updatableComparedCohorts = new Object2ObjectAVLTreeMap<>();
            comparedCohortWeights.forEach((k, v) -> {
                UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(precisionThreshold).build();
                comparedCohorts.put(k, sketch);
                updatableComparedCohorts.put(k, sketch);
            });
            UpdatableAggregationSketch as = new UpdatableAggregationSketch(timestamp, base, cohort, comparedCohorts, updatableComparedCohorts);
            this.aggregationSketches.add(as);
        }
        this.cohortWeight = cohortWeight;
        this.comparedCohortWeights = comparedCohortWeights;
        this.keyField = keyField;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        LongArrayValueSource keyLongArrayValueSource = new LongArrayValueSource(ctx, keyField);
        LongValueSource timeValueSource = new LongValueSource(ctx, timeField);
        final Bits cohortBits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), cohortWeight.scorerSupplier(ctx));
        Map<String, Bits> comparedCohortBits = new TreeMap<>();
        for (Entry<String, Weight> e : comparedCohortWeights.entrySet()) {
            comparedCohortBits.put(
                    e.getKey(),
                    Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), e.getValue().scorerSupplier(ctx)));
        }
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                long[] keyValue = keyLongArrayValueSource.getLongs(doc);
                Long timeValue = timeValueSource.getAsLong(doc);
                if (keyValue != null && timeValue != null && timeValue >= fromTimestamp && timeValue < toTimestamp) {
                    UpdatableAggregationSketch as = aggregationSketches.get(resolveId(timeValue));
                    as.update(cohortBits, comparedCohortBits, doc, keyValue);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrdinal) {
        return new InternalAggregation[]{new InternalRetention(
                name, metadata(),
                aggregationSketches)};
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRetention(name, metadata(), new ArrayList<>());
    }

    private int resolveId(long timestamp) {
        return (int) ((timestamp - fromTimestamp) / timestampInterval);
    }

    static class AggregationSketchUnion {

        private final long timestamp;
        private final Union base;
        private final Union cohort;
        private final Map<String, Union> comparedCohorts;

        AggregationSketchUnion(long timestamp) {
            this.timestamp = timestamp;
            this.base = SetOperation.builder().buildUnion();
            this.cohort = SetOperation.builder().buildUnion();
            this.comparedCohorts = new TreeMap<>();
        }

        void update(AggregationSketch as) {
            base.update(as.base);
            cohort.update(as.cohort);
            as.comparedCohorts.forEach((k, sketch) -> comparedCohorts
                    .computeIfAbsent(k, s -> SetOperation.builder().buildUnion()).update(sketch));
        }

        AggregationSketch toAggregationSketch() {
            Map<String, Sketch> m = new TreeMap<>();
            comparedCohorts.forEach((k, v) -> m.put(k, v.getResult()));
            return new AggregationSketch(timestamp, base.getResult(), cohort.getResult(), m);
        }

    }

    static class UpdatableAggregationSketch extends AggregationSketch {

        private final UpdateSketch updatableBase;
        private final UpdateSketch updatableCohort;
        private final Map<String, UpdateSketch> updatableComparedCohorts;

        private UpdatableAggregationSketch(long timestamp, UpdateSketch base, UpdateSketch cohort,
                                           Map<String, Sketch> comparedCohorts,
                                           Map<String, UpdateSketch> updatableComparedCohorts) {
            super(timestamp, base, cohort, comparedCohorts);
            this.updatableBase = base;
            this.updatableCohort = cohort;
            this.updatableComparedCohorts = updatableComparedCohorts;
        }

        private void update(Bits cohortBits, Map<String, Bits> comparedCohortBits, int docId, long[] value) {
            updatableBase.update(value);
            tryAddingDoc(cohortBits, docId, updatableCohort, value);
            for (Entry<String, UpdateSketch> entry : updatableComparedCohorts.entrySet()) {
                tryAddingDoc(comparedCohortBits.get(entry.getKey()), docId, entry.getValue(), value);
            }
        }

        private void tryAddingDoc(Bits bits, int docId,
                UpdateSketch sketch, long[] value) {
            if (bits.get(docId)) {
                sketch.update(value);
            }
        }

    }

    static class AggregationSketch {

        private final long timestamp;
        private final Sketch base;
        private final Sketch cohort;
        private final Map<String, Sketch> comparedCohorts;

        private AggregationSketch(long timestamp, Sketch base,
                                  Sketch cohort, Map<String, Sketch> comparedCohorts) {
            this.timestamp = timestamp;
            this.base = base;
            this.cohort = cohort;
            this.comparedCohorts = comparedCohorts;
        }

        long getTimestamp() {
            return timestamp;
        }

        Sketch getBase() {
            return base;
        }

        Sketch getCohort() {
            return cohort;
        }

        Map<String, Sketch> getComparedCohorts() {
            return comparedCohorts;
        }

        void doWriteTo(StreamOutput out) throws IOException {
            IOException e = Utils.doPrivileged(() -> {
                try {
                    out.writeVLong(timestamp);
                    out.writeByteArray(base.compact().toByteArray());
                    out.writeByteArray(cohort.compact().toByteArray());
                    out.writeVInt(comparedCohorts.size());
                    for (Entry<String, Sketch> spEntry : comparedCohorts.entrySet()) {
                        out.writeString(spEntry.getKey());
                        out.writeByteArray(spEntry.getValue().compact().toByteArray());
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

        static AggregationSketch doReadFrom(StreamInput in) throws IOException {
            long timestamp = in.readVLong();
            Sketch base = Sketches.wrapSketch(Memory.wrap(in.readByteArray()));
            Sketch cohort = Sketches.wrapSketch(Memory.wrap(in.readByteArray()));
            int size = in.readVInt();
            Map<String, Sketch> comparedCohorts = new Object2ObjectAVLTreeMap<>();
            for (int i = 0; i < size; i++) {
                String name = in.readString();
                Sketch sketch = Sketches.wrapSketch(Memory.wrap(in.readByteArray()));
                comparedCohorts.put(name, sketch);
            }
            return new AggregationSketch(timestamp, base, cohort, comparedCohorts);
        }

    }

}
