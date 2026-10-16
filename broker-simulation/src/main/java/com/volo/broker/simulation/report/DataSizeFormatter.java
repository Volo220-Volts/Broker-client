package com.volo.broker.simulation.report;

import java.util.Locale;

public final class DataSizeFormatter {
    private static final String[] UNITS = {"B", "KiB", "MiB", "GiB", "TiB"};

    private DataSizeFormatter() {
    }

    public static String bytes(double bytes) {
        double value = bytes;
        int unit = 0;

        while (value >= 1024 && unit < UNITS.length - 1) {
            value /= 1024;
            unit++;
        }

        return unit == 0
                ? String.format(Locale.ROOT, "%.0f %s", value, UNITS[unit])
                : String.format(Locale.ROOT, "%.2f %s", value, UNITS[unit]);
    }

    public static String bytesPerSecond(double bytesPerSecond) {
        return bytes(bytesPerSecond) + "/s";
    }
}

