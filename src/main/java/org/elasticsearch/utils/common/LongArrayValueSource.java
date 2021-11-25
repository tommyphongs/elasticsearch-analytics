package org.elasticsearch.utils.common;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;

import java.io.IOException;


public class LongArrayValueSource {

    private final DocValuesType type;
    private final DocIdSetIterator iterator;

    public LongArrayValueSource(LeafReaderContext leafReaderContext, String field) throws IOException {
        LeafReader reader = leafReaderContext.reader();
        if (reader.getFieldInfos().fieldInfo(field) == null) {
            throw new IllegalArgumentException("Field " + field + " is not exists");
        }

        type = reader.getFieldInfos().fieldInfo(field).getDocValuesType();

        switch (type) {
            case SORTED_SET:
                iterator = reader.getSortedSetDocValues(field);
                break;
            case SORTED_NUMERIC:
                iterator = reader.getSortedNumericDocValues(field);
                break;
            default:
                throw new IllegalArgumentException("Field type " +
                        reader.getFieldInfos().fieldInfo(field).getDocValuesType().toString() + " is not supported");
        }
    }

    public long[] getLongs(int doc) throws IOException {
        switch (type) {
            case SORTED_SET:
                SortedSetDocValues ssdv = (SortedSetDocValues) iterator;
                if (ssdv.advanceExact(doc)) {
                    LongOpenHashSet l = new LongOpenHashSet();
                    long next;
                    final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                    while ((next = ssdv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                        BytesRef value = ssdv.lookupOrd(next);
                        l.add(MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash).h1);
                    }
                    return l.toLongArray();
                } else {
                    return null;
                }
            case SORTED_NUMERIC:
                SortedNumericDocValues sndv = (SortedNumericDocValues) iterator;
                if (sndv.advanceExact(doc)) {
                    long[] data = new long[sndv.docValueCount()];
                    for (int i = 0; i < sndv.docValueCount(); i++) {
                        data[i] = sndv.nextValue();
                    }
                    return data;
                } else {
                    return null;
                }
            default:
                throw new IllegalArgumentException("This should never happen");
        }
    }

}
