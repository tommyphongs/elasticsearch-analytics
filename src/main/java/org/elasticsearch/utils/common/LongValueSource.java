package org.elasticsearch.utils.common;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;


public class LongValueSource {

    private final SortedNumericDocValues valuesIterator;

    public LongValueSource(LeafReaderContext leafReaderContext, String field) throws IOException {
        if (leafReaderContext.reader().getFieldInfos().fieldInfo(field) == null) {
            throw new IllegalArgumentException("Field " + field + " is not exists");
        }
        if (leafReaderContext.reader().getFieldInfos().fieldInfo(field).getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
            this.valuesIterator = leafReaderContext.reader().getSortedNumericDocValues(field);
            if (valuesIterator == null) {
                throw new IllegalArgumentException("Field " + field + " is not exists");
            }
        } else {
            throw new IllegalArgumentException("Field type " +
                    leafReaderContext.reader().getFieldInfos().fieldInfo(field).getDocValuesType().toString() + " is not supported");
        }
    }

    public Long getAsLong(int doc) throws IOException {
        boolean hasValue = valuesIterator.advanceExact(doc);
        if (hasValue) {
            if (valuesIterator.docValueCount() > 0)
                return valuesIterator.nextValue();
        }
        return null;
    }

}
