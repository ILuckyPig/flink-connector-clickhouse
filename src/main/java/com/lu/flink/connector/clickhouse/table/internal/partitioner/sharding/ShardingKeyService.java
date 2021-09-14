package com.lu.flink.connector.clickhouse.table.internal.partitioner.sharding;

import org.apache.flink.table.api.ValidationException;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class ShardingKeyService {
    public static <T extends ShardingKey> T find(Class<T> shardingKeyClass, String identifier) {
        return findSingleInternal(shardingKeyClass, identifier);
    }

    public static <T> List<T> find(Class<T> shardingKeyClass) {
        List<ShardingKey> shardingKeys = discoverShardingKeys();
        return filterByShardingKeyClass(shardingKeyClass, shardingKeys);
    }

    private static <T extends ShardingKey> T findSingleInternal(Class<T> shardingKeyClass, String identifier) {
        List<ShardingKey> shardingKeys = discoverShardingKeys();
        List<T> filtered = filter(shardingKeys, shardingKeyClass, identifier);

        if (filtered.size() > 1) {
            throw new RuntimeException(
                    filtered +
                            shardingKeys.toString() +
                            shardingKeys +
                            identifier);
        } else {
            return filtered.get(0);
        }
    }

    private static List<ShardingKey> discoverShardingKeys() {
        List<ShardingKey> result = new LinkedList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(ShardingKey.class, classLoader).iterator().forEachRemaining(result::add);
        return result;
    }

    private static <T extends ShardingKey> List<T> filter(List<ShardingKey> foundShardingKeys,
                                                          Class<T> shardingKeyClass,
                                                          String identifier) {
        List<T> filterByShardingKeyClass = filterByShardingKeyClass(shardingKeyClass, identifier, foundShardingKeys);

        return filterByIdentifier(shardingKeyClass, identifier, filterByShardingKeyClass);
    }

    private static <T> List<T> filterByShardingKeyClass(Class<T> shardingKeyClass,
                                                        String identifier,
                                                        List<ShardingKey> foundShardingKeys) {
        List<T> filterByShardingKeyClass = filterByShardingKeyClass(shardingKeyClass, foundShardingKeys);

        if (filterByShardingKeyClass.isEmpty()) {
            throw new RuntimeException(
                    String.format("No factory implements '%s'.", shardingKeyClass.getCanonicalName()) +
                            shardingKeyClass +
                            foundShardingKeys +
                            identifier);
        }

        return filterByShardingKeyClass;
    }

    private static <T> List<T> filterByShardingKeyClass(Class<T> shardingKeyClass,
                                                                  List<ShardingKey> foundShardingKeys) {
        return (List<T>) foundShardingKeys.stream()
                .filter(p -> shardingKeyClass.isAssignableFrom(p.getClass()))
                .collect(Collectors.toList());
    }

    private static <T extends ShardingKey> List<T> filterByIdentifier(Class<T> shardingKeyClass,
                                                                      String identifier,
                                                                      List<T> classes) {
        List<T> matchingShardingKeys = classes.stream()
                .filter(f -> f.identifier().equals(identifier))
                .collect(Collectors.toList());

        if (matchingShardingKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any sharding key for identifier '%s' that implements '%s' in the classpath.\n\n" +
                                    "Available sharding key identifiers are:\n\n" +
                                    "%s",
                            identifier,
                            shardingKeyClass.getName(),
                            classes.stream()
                                    .map(ShardingKey::identifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingShardingKeys.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n" +
                                    "Ambiguous sharding key classes are:\n\n" +
                                    "%s",
                            identifier,
                            shardingKeyClass.getName(),
                            matchingShardingKeys.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingShardingKeys;
    }
}
