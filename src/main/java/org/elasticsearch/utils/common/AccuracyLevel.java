package org.elasticsearch.utils.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public enum AccuracyLevel {
    LOW("low", 4096),
    MEDIUM("medium", 65536),
    HIGH("high", 524288);

    private static final Map<String, AccuracyLevel> NAME_MAP;

    static {
        Map<String, AccuracyLevel> m = new HashMap<>();
        for (AccuracyLevel level : values()) {
            m.put(level.getValue(), level);
        }
        NAME_MAP = Collections.unmodifiableMap(m);
    }

    private final String value;
    private final int nominalEntries;

    AccuracyLevel(String value, int nominalEntries) {
        this.value = value;
        this.nominalEntries = nominalEntries;
    }

    public static AccuracyLevel getFromName(String name) {
        Objects.requireNonNull(name);
        String lowercase = name.toLowerCase();
        if (!NAME_MAP.containsKey(lowercase)) {
            throw new IllegalArgumentException("Accuracy level with name " + name + " is not supported");
        }
        return NAME_MAP.get(lowercase);
    }

    public String getValue() {
        return value;
    }

    public int getNominalEntries() {
        return nominalEntries;
    }

}
