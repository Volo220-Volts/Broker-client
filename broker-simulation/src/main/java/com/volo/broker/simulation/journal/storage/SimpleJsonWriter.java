package com.volo.broker.simulation.journal.storage;

import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public final class SimpleJsonWriter {
    private SimpleJsonWriter() {
    }

    public static String toJson(Object obj) {
        StringBuilder sb = new StringBuilder(4096);
        write(obj, sb, 0);
        return sb.toString();
    }

    private static void write(Object obj, StringBuilder sb, int indentLevel) {
        if (obj == null) {
            sb.append("null");
            return;
        }

        if (obj instanceof String s) {
            writeString(s, sb);
            return;
        }

        if (obj instanceof Number || obj instanceof Boolean) {
            sb.append(obj);
            return;
        }

        if (obj instanceof Enum<?> e) {
            writeString(e.name(), sb);
            return;
        }

        if (obj instanceof TemporalAccessor) {
            writeString(obj.toString(), sb);
            return;
        }

        if (obj instanceof Map<?, ?> map) {
            writeMap(map, sb, indentLevel);
            return;
        }

        if (obj instanceof Iterable<?> iterable) {
            writeIterable(iterable, sb, indentLevel);
            return;
        }

        if (obj.getClass().isArray()) {
            writeArray(obj, sb, indentLevel);
            return;
        }

        if (obj.getClass().isRecord()) {
            writeRecord(obj, sb, indentLevel);
            return;
        }

        // Fallback toString as quoted string
        writeString(obj.toString(), sb);
    }

    private static void writeRecord(Object record, StringBuilder sb, int indentLevel) {
        RecordComponent[] components = record.getClass().getRecordComponents();
        if (components == null || components.length == 0) {
            sb.append("{}");
            return;
        }

        sb.append("{\n");
        int nextIndent = indentLevel + 1;

        for (int i = 0; i < components.length; i++) {
            RecordComponent comp = components[i];
            indent(sb, nextIndent);
            writeString(comp.getName(), sb);
            sb.append(": ");

            try {
                Object val = comp.getAccessor().invoke(record);
                write(val, sb, nextIndent);
            } catch (Throwable e) {
                sb.append("null");
            }

            if (i < components.length - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }

        indent(sb, indentLevel);
        sb.append("}");
    }

    private static void writeMap(Map<?, ?> map, StringBuilder sb, int indentLevel) {
        if (map.isEmpty()) {
            sb.append("{}");
            return;
        }

        sb.append("{\n");
        int nextIndent = indentLevel + 1;
        int i = 0;
        int size = map.size();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            indent(sb, nextIndent);
            writeString(String.valueOf(entry.getKey()), sb);
            sb.append(": ");
            write(entry.getValue(), sb, nextIndent);

            if (++i < size) {
                sb.append(",");
            }
            sb.append("\n");
        }

        indent(sb, indentLevel);
        sb.append("}");
    }

    private static void writeIterable(Iterable<?> iterable, StringBuilder sb, int indentLevel) {
        Iterator<?> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            sb.append("[]");
            return;
        }

        sb.append("[\n");
        int nextIndent = indentLevel + 1;

        while (iterator.hasNext()) {
            Object item = iterator.next();
            indent(sb, nextIndent);
            write(item, sb, nextIndent);

            if (iterator.hasNext()) {
                sb.append(",");
            }
            sb.append("\n");
        }

        indent(sb, indentLevel);
        sb.append("]");
    }

    private static void writeArray(Object array, StringBuilder sb, int indentLevel) {
        int length = Array.getLength(array);
        if (length == 0) {
            sb.append("[]");
            return;
        }

        sb.append("[\n");
        int nextIndent = indentLevel + 1;

        for (int i = 0; i < length; i++) {
            indent(sb, nextIndent);
            write(Array.get(array, i), sb, nextIndent);

            if (i < length - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }

        indent(sb, indentLevel);
        sb.append("]");
    }

    private static void writeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 32) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static void indent(StringBuilder sb, int count) {
        sb.append("  ".repeat(Math.max(0, count)));
    }
}

