package org.elasticsearch.utils.common;

import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.TreeMap;

public class Utils {

    // Wrap AccessController.doPrivileged with SecurityManager check
    public static <T> T doPrivileged(PrivilegedAction<T> action) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged(action);
    }

    public static double estimateIntersection(Sketch a, Sketch b) {
        if (a == null || b == null) {
            return 0.0D;
        }
        Intersection intersection = SetOperation.builder().buildIntersection();
        intersection.update(a);
        intersection.update(b);
        return intersection.getResult().getEstimate();
    }

    public static void writeString2QbMap(StreamOutput out, Map<String, QueryBuilder> m) throws IOException {
        out.writeVInt(m.size());
        for (Map.Entry<String, QueryBuilder> entry : m.entrySet()) {
            out.writeString(entry.getKey());
            out.writeNamedWriteable(entry.getValue());
        }
    }

    public static Map<String, QueryBuilder> readString2QbMap(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, QueryBuilder> m = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String k = in.readString();
            QueryBuilder qb = in.readNamedWriteable(QueryBuilder.class);
            m.put(k, qb);
        }
        return m;
    }

}
