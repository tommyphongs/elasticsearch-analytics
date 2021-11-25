package org.elasticsearch.utils;

import org.elasticsearch.utils.aggs.cardjoin.CardinalityJoinAggregationBuilder;
import org.elasticsearch.utils.aggs.cardjoin.InternalCardinalityJoin;
import org.elasticsearch.utils.aggs.retention.InternalRetention;
import org.elasticsearch.utils.aggs.retention.RetentionAggregationBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.List;


public class AnalyticsPlugins extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        ArrayList<AggregationSpec> aggregationSpecs = new ArrayList<>();
        AggregationSpec queryJoinSpec = new AggregationSpec(
                CardinalityJoinAggregationBuilder.NAME,
                CardinalityJoinAggregationBuilder::new,
                CardinalityJoinAggregationBuilder::parse
        ).addResultReader(
                InternalCardinalityJoin::new
        );
        aggregationSpecs.add(queryJoinSpec);
        AggregationSpec retentionSpec =
                new AggregationSpec(
                        RetentionAggregationBuilder.NAME,
                        RetentionAggregationBuilder::new,
                        RetentionAggregationBuilder::parse).addResultReader(
                        InternalRetention::new
                );
        aggregationSpecs.add(retentionSpec);
        return aggregationSpecs;
    }

}


