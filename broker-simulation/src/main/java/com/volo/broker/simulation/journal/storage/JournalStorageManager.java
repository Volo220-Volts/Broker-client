package com.volo.broker.simulation.journal.storage;

import com.volo.broker.simulation.config.BrokerType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class JournalStorageManager {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
    private static final Pattern SEQUENTIAL_PATTERN = Pattern.compile("^sim[._]?(\\d+)$");

    private final Path baseDirectory;
    private final JournalNamingStrategy namingStrategy;

    public record RunDirectory(
            String simulationId,
            Path directory
    ) {
    }

    public JournalStorageManager(Path baseDirectory, JournalNamingStrategy namingStrategy) {
        this.baseDirectory = Objects.requireNonNull(baseDirectory, "baseDirectory is required");
        this.namingStrategy = namingStrategy != null ? namingStrategy : JournalNamingStrategy.TIMESTAMP;
    }

    public JournalStorageManager(Path baseDirectory) {
        this(baseDirectory, JournalNamingStrategy.TIMESTAMP);
    }

    public JournalStorageManager(String baseDirectoryPath, JournalNamingStrategy namingStrategy) {
        this(Path.of(baseDirectoryPath == null || baseDirectoryPath.isBlank() ? "simulation" : baseDirectoryPath), namingStrategy);
    }

    public JournalStorageManager(String baseDirectoryPath) {
        this(baseDirectoryPath, JournalNamingStrategy.TIMESTAMP);
    }

    public JournalNamingStrategy namingStrategy() {
        return namingStrategy;
    }

    public Path baseDirectory() {
        return baseDirectory;
    }

    public List<SimulationRunInfo> listExistingRuns(BrokerType brokerType) {
        String brokerDirName = resolveBrokerDirName(brokerType);
        Path brokerDirectory = baseDirectory.resolve(brokerDirName);

        if (!Files.exists(brokerDirectory) || !Files.isDirectory(brokerDirectory)) {
            return List.of();
        }

        try (Stream<Path> stream = Files.list(brokerDirectory)) {
            List<SimulationRunInfo> runs = stream
                    .filter(Files::isDirectory)
                    .filter(JournalStorageManager::isSimulationRunDirectory)
                    .map(path -> toRunInfo(path, brokerType))
                    .sorted(Comparator.comparing(SimulationRunInfo::lastModified))
                    .toList();

            return runs;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list existing simulation runs in " + brokerDirectory, e);
        }
    }

    public List<SimulationRunInfo> listAllRuns() {
        List<SimulationRunInfo> allRuns = new ArrayList<>();
        for (BrokerType brokerType : BrokerType.values()) {
            allRuns.addAll(listExistingRuns(brokerType));
        }
        allRuns.sort(Comparator.comparing(SimulationRunInfo::lastModified));
        return Collections.unmodifiableList(allRuns);
    }

    public RunDirectory allocateCustomRunDirectory(BrokerType brokerType, String customName) {
        if (customName == null || customName.isBlank()) {
            return allocateNextRunDirectory(brokerType);
        }

        String brokerDirName = resolveBrokerDirName(brokerType);
        Path brokerDirectory = baseDirectory.resolve(brokerDirName);

        try {
            Files.createDirectories(brokerDirectory);

            String sanitized = sanitizeRunName(customName);
            String runId = sanitized;
            Path runDir = brokerDirectory.resolve(runId);

            int collisionCounter = 2;
            while (Files.exists(runDir)) {
                runId = sanitized + "_" + collisionCounter++;
                runDir = brokerDirectory.resolve(runId);
            }

            Files.createDirectories(runDir);
            return new RunDirectory(runId, runDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to allocate custom simulation run directory in " + brokerDirectory, e);
        }
    }

    public RunDirectory allocateNextRunDirectory(BrokerType brokerType) {
        String brokerDirName = resolveBrokerDirName(brokerType);
        Path brokerDirectory = baseDirectory.resolve(brokerDirName);

        try {
            Files.createDirectories(brokerDirectory);

            if (namingStrategy == JournalNamingStrategy.SEQUENTIAL) {
                return allocateSequentialRunDirectory(brokerDirectory);
            } else {
                return allocateTimestampRunDirectory(brokerDirectory);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to allocate simulation run directory in " + brokerDirectory, e);
        }
    }

    private RunDirectory allocateTimestampRunDirectory(Path brokerDirectory) throws IOException {
        String baseTimestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        String baseRunId = "sim_" + baseTimestamp;
        String runId = baseRunId;
        Path runDir = brokerDirectory.resolve(runId);

        int collisionCounter = 2;
        while (Files.exists(runDir)) {
            runId = baseRunId + "_" + collisionCounter++;
            runDir = brokerDirectory.resolve(runId);
        }

        Files.createDirectories(runDir);
        return new RunDirectory(runId, runDir);
    }

    private RunDirectory allocateSequentialRunDirectory(Path brokerDirectory) throws IOException {
        int nextIndex = 1;
        try (Stream<Path> stream = Files.list(brokerDirectory)) {
            int maxFound = stream
                    .filter(Files::isDirectory)
                    .map(path -> path.getFileName().toString())
                    .map(SEQUENTIAL_PATTERN::matcher)
                    .filter(Matcher::matches)
                    .mapToInt(matcher -> Integer.parseInt(matcher.group(1)))
                    .max()
                    .orElse(0);

            nextIndex = maxFound + 1;
        }

        String runId = "sim." + nextIndex;
        Path runDir = brokerDirectory.resolve(runId);
        Files.createDirectories(runDir);

        return new RunDirectory(runId, runDir);
    }

    private static SimulationRunInfo toRunInfo(Path path, BrokerType brokerType) {
        String runId = path.getFileName().toString();
        Instant lastModified = Instant.EPOCH;
        try {
            lastModified = Files.getLastModifiedTime(path).toInstant();
        } catch (IOException ignored) {
        }

        boolean hasJournal = Files.exists(path.resolve("journal.json"));
        boolean hasSummary = Files.exists(path.resolve("summary.txt"));
        boolean hasTimeline = Files.exists(path.resolve("timeline.csv"));
        boolean hasConfig = Files.exists(path.resolve("config.json"));

        return new SimulationRunInfo(
                runId,
                brokerType,
                path,
                lastModified,
                hasJournal,
                hasSummary,
                hasTimeline,
                hasConfig
        );
    }

    private static boolean isSimulationRunDirectory(Path path) {
        String name = path.getFileName().toString();
        return name.startsWith("sim")
                || Files.exists(path.resolve("journal.json"))
                || Files.exists(path.resolve("summary.txt"))
                || Files.exists(path.resolve("config.json"))
                || Files.exists(path.resolve("timeline.csv"));
    }

    private static String sanitizeRunName(String name) {
        String sanitized = name.trim().replaceAll("[/\\\\:*?\"<>|\\s]+", "_");
        return sanitized.isEmpty() ? "sim_custom" : sanitized;
    }

    private static String resolveBrokerDirName(BrokerType brokerType) {
        return (brokerType == null ? BrokerType.CUSTOM : brokerType).name().toLowerCase();
    }
}
