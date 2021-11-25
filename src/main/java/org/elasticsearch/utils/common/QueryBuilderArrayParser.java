package org.elasticsearch.utils.common;

import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class QueryBuilderArrayParser implements ContextParser<XContentParser, Void> {

    @Override
    public Void parse(XContentParser p, XContentParser c) throws IOException {
        return null;
    }

}
