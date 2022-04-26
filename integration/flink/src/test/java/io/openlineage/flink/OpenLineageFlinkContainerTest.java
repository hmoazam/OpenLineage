package io.openlineage.flink;

import static java.nio.file.Files.readAllBytes;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.google.common.io.Resources;
import java.nio.file.Path;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration-test")
@Testcontainers
@Slf4j
public class OpenLineageFlinkContainerTest {

  private static final Network network = Network.newNetwork();
  private static MockServerClient mockServerClient;

  @Container
  private static final MockServerContainer openLineageClientMockContainer =
      FlinkContainerUtils.makeMockServerContainer(network);

  @Container
  private static final GenericContainer zookeeper =
      FlinkContainerUtils.makeZookeeperContainer(network);

  @Container
  private static final GenericContainer kafka =
      FlinkContainerUtils.makeKafkaContainer(network, zookeeper);

  @Container
  private static final GenericContainer schemaRegistry =
      FlinkContainerUtils.makeSchemaRegistryContainer(network, kafka);

  @Container
  private static final GenericContainer initKafka =
      FlinkContainerUtils.makeInitKafkaContainer(network, schemaRegistry);

  @Container
  private static final GenericContainer generateEvents =
      FlinkContainerUtils.makeGenerateEventsContainer(network, initKafka);

  private static final GenericContainer jobManager =
      FlinkContainerUtils.makeFlinkJobManagerContainer(
          network, Arrays.asList(generateEvents, openLineageClientMockContainer));

  private static final GenericContainer taskManager =
      FlinkContainerUtils.makeFlinkTaskManagerContainer(network, Arrays.asList(jobManager));

  @BeforeAll
  public static void setup() {
    mockServerClient =
        new MockServerClient(
            openLineageClientMockContainer.getHost(),
            openLineageClientMockContainer.getServerPort());
    mockServerClient
        .when(request("/api/v1/lineage"))
        .respond(org.mockserver.model.HttpResponse.response().withStatusCode(201));

    Awaitility.await().until(openLineageClientMockContainer::isRunning);
  }

  @AfterAll
  public static void tearDown() {
    try {
      openLineageClientMockContainer.stop();
    } catch (Exception e) {
    }
    try {
      zookeeper.stop();
    } catch (Exception e) {
    }
    try {
      schemaRegistry.stop();
    } catch (Exception e) {
    }
    try {
      kafka.stop();
    } catch (Exception e) {
    }
    try {
      jobManager.stop();
    } catch (Exception e) {
    }
    try {
      taskManager.stop();
    } catch (Exception e) {
    }
    try {
      initKafka.stop();
    } catch (Exception e) {
    }

    network.close();
  }

  @Test
  @SneakyThrows
  public void testOpenLineageEventSent() {
    // taskManager container should depend on jobManager, but we want to finish based on action
    // occurred in jobManager
    new Thread(
            () -> {
              jobManager.start();
              taskManager.stop();
            })
        .run();
    taskManager.start();

    mockServerClient.verify(
        request()
            .withPath("/api/v1/lineage")
            .withBody(
                readJson(Path.of(Resources.getResource("events/expected_event.json").getPath()))));
  }

  @SneakyThrows
  private JsonBody readJson(Path path) {
    return json(new String(readAllBytes(path)), MatchType.ONLY_MATCHING_FIELDS);
  }
}
