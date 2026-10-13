package com.volo.broker.simulation.report;

import java.util.ArrayList;
import java.util.List;

public final class ConsoleTable {
    private final String title;
    private final List<String> headers;
    private final List<List<String>> rows = new ArrayList<>();

    public ConsoleTable(String title, String... headers) {
        this.title = title;
        this.headers = List.of(headers);
    }

    public ConsoleTable addRow(String... cells) {
        if (cells.length != headers.size()) {
            throw new IllegalArgumentException("row must contain " + headers.size() + " cells");
        }

        rows.add(List.of(cells));

        return this;
    }

    public String render() {
        int[] widths = columnWidths();
        StringBuilder out = new StringBuilder();

        if (title != null && !title.isBlank()) {
            out.append(title).append(System.lineSeparator());
        }

        out.append(separator('┌', '┬', '┐', widths));
        out.append(row(headers, widths, false));
        out.append(separator('├', '┼', '┤', widths));

        for (List<String> row : rows) {
            out.append(row(row, widths, true));
        }

        out.append(separator('└', '┴', '┘', widths));

        return out.toString();
    }

    private int[] columnWidths() {
        int[] widths = new int[headers.size()];

        for (int i = 0; i < widths.length; i++) {
            widths[i] = headers.get(i).length();
        }

        for (List<String> row : rows) {
            for (int i = 0; i < widths.length; i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        return widths;
    }

    private String separator(char left, char middle, char right, int[] widths) {
        StringBuilder line = new StringBuilder().append(left);

        for (int i = 0; i < widths.length; i++) {
            line.append("─".repeat(widths[i] + 2));
            line.append(i == widths.length - 1 ? right : middle);
        }

        return line.append(System.lineSeparator()).toString();
    }

    private String row(List<String> cells, int[] widths, boolean alignRight) {
        StringBuilder line = new StringBuilder("│");

        for (int i = 0; i < widths.length; i++) {
            String cell = cells.get(i);
            String padding = " ".repeat(widths[i] - cell.length());

            line.append(' ');
            line.append(alignRight && i > 0 ? padding + cell : cell + padding);
            line.append(" │");
        }

        return line.append(System.lineSeparator()).toString();
    }
}

