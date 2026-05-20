package com.realrisk.tools;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class DlqReplayTool {
  private DlqReplayTool() {}

  public static void main(String[] args) throws Exception {
    Options options = Options.parse(args);

    if (options.execute() && (options.operator() == null || options.operator().isBlank())) {
      throw new IllegalArgumentException("--operator is required when --execute is set");
    }

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps(options))) {
      consumer.subscribe(List.of(options.sourceTopic()));

      waitForAssignment(consumer);

      int matched = 0;
      int replayed = 0;
      List<String> summaries = new ArrayList<>();

      KafkaProducer<byte[], byte[]> producer =
          options.execute() ? new KafkaProducer<>(producerProps(options)) : null;
      try {
        int idlePolls = 0;
        while (matched < options.maxMessages() && idlePolls < options.maxIdlePolls()) {
          ConsumerRecords<byte[], byte[]> records = consumer.poll(options.pollTimeout());
          if (records.isEmpty()) {
            idlePolls++;
            continue;
          }

          idlePolls = 0;
          for (ConsumerRecord<byte[], byte[]> record : records) {
            if (!matches(record, options)) {
              continue;
            }

            matched++;
            String summary = summarize(record);
            summaries.add(summary);
            System.out.println(summary);

            if (options.execute()) {
              ProducerRecord<byte[], byte[]> replayRecord = replayRecord(record, options);
              Objects.requireNonNull(producer).send(replayRecord).get();
              replayed++;
            }

            if (matched >= options.maxMessages()) {
              break;
            }
          }
        }

        if (options.execute()) {
          Objects.requireNonNull(producer).flush();
        }

        if (matched == 0) {
          System.out.println("No DLQ messages matched the supplied filters.");
        } else if (!options.execute()) {
          System.out.printf("Dry run matched %d message(s). Nothing was replayed.%n", matched);
        } else {
          System.out.printf(
              "Replay complete. Matched %d message(s); replayed %d to %s.%n",
              matched, replayed, options.targetTopic());
        }
      } finally {
        if (producer != null) {
          producer.close(Duration.ofSeconds(5));
        }
      }
    }
  }

  private static void waitForAssignment(KafkaConsumer<byte[], byte[]> consumer)
      throws InterruptedException {
    int attempts = 0;
    while (consumer.assignment().isEmpty() && attempts < 40) {
      consumer.poll(Duration.ofMillis(250));
      Thread.sleep(100L);
      attempts++;
    }
  }

  private static boolean matches(ConsumerRecord<byte[], byte[]> record, Options options) {
    String originalTopic = header(record.headers(), "x-original-topic");
    if (options.originalTopic() != null && !options.originalTopic().equals(originalTopic)) {
      return false;
    }

    String severity = header(record.headers(), "severity");
    if (options.severity() != null
        && (severity == null || !options.severity().equalsIgnoreCase(severity))) {
      return false;
    }

    if (options.failedAfter() != null || options.failedBefore() != null) {
      String failedAtHeader = header(record.headers(), "x-failed-at");
      if (failedAtHeader == null) {
        return false;
      }

      Instant failedAt;
      try {
        failedAt = Instant.parse(failedAtHeader);
      } catch (DateTimeParseException ignored) {
        return false;
      }

      if (options.failedAfter() != null && failedAt.isBefore(options.failedAfter())) {
        return false;
      }
      if (options.failedBefore() != null && failedAt.isAfter(options.failedBefore())) {
        return false;
      }
    }

    return true;
  }

  private static String summarize(ConsumerRecord<byte[], byte[]> record) {
    String originalTopic = header(record.headers(), "x-original-topic");
    String failedAt = header(record.headers(), "x-failed-at");
    String retryCount = header(record.headers(), "x-retry-count");
    String severity = header(record.headers(), "severity");
    int keySize = record.key() == null ? 0 : record.key().length;
    int valueSize = record.value() == null ? 0 : record.value().length;
    return String.format(
        "match topic=%s partition=%d offset=%d originalTopic=%s failedAt=%s severity=%s retryCount=%s keyBytes=%d valueBytes=%d",
        record.topic(),
        record.partition(),
        record.offset(),
        nullToDash(originalTopic),
        nullToDash(failedAt),
        nullToDash(severity),
        nullToDash(retryCount),
        keySize,
        valueSize);
  }

  private static ProducerRecord<byte[], byte[]> replayRecord(
      ConsumerRecord<byte[], byte[]> record, Options options) {
    Headers headers = copyHeaders(record.headers());
    upsert(headers, "x-replayed-at", Instant.now().toString());
    upsert(headers, "x-replayed-by", options.operator());
    return new ProducerRecord<>(
        options.targetTopic(), null, record.key(), record.value(), headers);
  }

  private static Headers copyHeaders(Headers headers) {
    RecordHeaders copy = new RecordHeaders();
    for (Header header : headers) {
      copy.add(header.key(), header.value());
    }
    return copy;
  }

  private static void upsert(Headers headers, String key, String value) {
    headers.remove(key);
    headers.add(key, value.getBytes(StandardCharsets.UTF_8));
  }

  private static String header(Headers headers, String key) {
    Header header = headers.lastHeader(key);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  private static String nullToDash(String value) {
    return value == null || value.isBlank() ? "-" : value;
  }

  private static Properties consumerProps(Options options) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-replay-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }

  private static Properties producerProps(Options options) {
    Properties props = new Properties();
    props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        options.bootstrapServers());
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    return props;
  }

  record Options(
      String bootstrapServers,
      String sourceTopic,
      String targetTopic,
      String originalTopic,
      Instant failedAfter,
      Instant failedBefore,
      String severity,
      boolean execute,
      String operator,
      int maxMessages,
      int maxIdlePolls,
      Duration pollTimeout) {
    static Options parse(String[] args) {
      String bootstrapServers = "localhost:9092";
      String sourceTopic = "alert-events-dlq";
      String targetTopic = "alert-events";
      String originalTopic = null;
      Instant failedAfter = null;
      Instant failedBefore = null;
      String severity = null;
      boolean execute = false;
      String operator = null;
      int maxMessages = 100;
      int maxIdlePolls = 8;
      Duration pollTimeout = Duration.ofMillis(500);

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        switch (arg) {
          case "--bootstrap-servers" -> bootstrapServers = requireValue(args, ++i, arg);
          case "--source-topic" -> sourceTopic = requireValue(args, ++i, arg);
          case "--target-topic" -> targetTopic = requireValue(args, ++i, arg);
          case "--original-topic" -> originalTopic = requireValue(args, ++i, arg);
          case "--failed-after" -> failedAfter = Instant.parse(requireValue(args, ++i, arg));
          case "--failed-before" -> failedBefore = Instant.parse(requireValue(args, ++i, arg));
          case "--severity" -> severity = requireValue(args, ++i, arg);
          case "--operator" -> operator = requireValue(args, ++i, arg);
          case "--max-messages" -> maxMessages = Integer.parseInt(requireValue(args, ++i, arg));
          case "--max-idle-polls" ->
              maxIdlePolls = Integer.parseInt(requireValue(args, ++i, arg));
          case "--poll-timeout-ms" ->
              pollTimeout = Duration.ofMillis(Long.parseLong(requireValue(args, ++i, arg)));
          case "--execute" -> execute = true;
          case "--help" -> {
            printUsage();
            System.exit(0);
          }
          default -> throw new IllegalArgumentException("Unknown argument: " + arg);
        }
      }

      return new Options(
          bootstrapServers,
          sourceTopic,
          targetTopic,
          originalTopic,
          failedAfter,
          failedBefore,
          severity,
          execute,
          operator,
          maxMessages,
          maxIdlePolls,
          pollTimeout);
    }

    private static String requireValue(String[] args, int index, String flag) {
      if (index >= args.length) {
        throw new IllegalArgumentException("Missing value for " + flag);
      }
      return args[index];
    }

    private static void printUsage() {
      System.out.println(
          """
          Replay records from alert-events-dlq back into alert-events.

          Options:
            --bootstrap-servers <host:port>   Kafka bootstrap servers (default: localhost:9092)
            --source-topic <topic>            DLQ topic to read (default: alert-events-dlq)
            --target-topic <topic>            Replay target topic (default: alert-events)
            --original-topic <topic>          Filter by x-original-topic header
            --failed-after <ISO-8601>         Only replay records failed at/after this instant
            --failed-before <ISO-8601>        Only replay records failed at/before this instant
            --severity <value>                Filter by severity header when present
            --max-messages <n>                Stop after n matched records (default: 100)
            --max-idle-polls <n>              Stop after n empty polls (default: 8)
            --poll-timeout-ms <ms>            Per-poll timeout (default: 500)
            --execute                         Actually publish matching records
            --operator <name>                 Required with --execute; used for x-replayed-by
            --help                            Show this message
          """);
    }
  }
}
