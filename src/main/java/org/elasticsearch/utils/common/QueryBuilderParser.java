package org.elasticsearch.utils.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class QueryBuilderParser implements Writeable, ToXContentFragment {

    public static final ObjectParser.NamedObjectParser<QueryBuilderParser, Void> PARSER =
            (XContentParser p, Void c, String name) ->
                    new QueryBuilderParser(name, parseInnerQueryBuilder(p));
    private final String key;
    private final QueryBuilder filter;

    public QueryBuilderParser(String key, QueryBuilder filter) {
        if (key == null) {
            throw new IllegalArgumentException("[key] must not be null");
        }
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null");
        }

        this.key = key;
        this.filter = filter;
    }

    /**
     * Read from a stream.
     */
    public QueryBuilderParser(StreamInput in) throws IOException {
        key = in.readString();
        filter = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeNamedWriteable(filter);
    }

    public String key() {
        return key;
    }

    public QueryBuilder getQueryBuilder() {
        return filter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(key, filter);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        QueryBuilderParser other = (QueryBuilderParser) obj;
        return Objects.equals(key, other.key) && Objects.equals(filter, other.filter);
    }
}
