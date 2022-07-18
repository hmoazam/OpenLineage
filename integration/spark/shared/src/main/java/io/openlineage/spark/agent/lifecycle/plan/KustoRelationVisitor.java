package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;

/** */
@Slf4j
public class KustoRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {

  private final DatasetFactory<D> factory;
  private static final String KUSTO_CLASS_NAME =
      "com.microsoft.kusto.spark.datasource.KustoRelation";

  private static final String KUSTO_PROVIDER_CLASS_NAME =
      "com.microsoft.kusto.spark.datasource.DefaultSource"; // Not used

  private static final String KUSTO_URL_PREFIX = "https://";
  private static final String KUSTO_URL_SUFFIX = ".kusto.windows.net";

  private static final String KUSTO_PREFIX = "azurekusto://";

  public KustoRelationVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  public static boolean isKustoClass(LogicalPlan plan) {
    return plan instanceof LogicalRelation
        && ((LogicalRelation) plan).relation().getClass().getName().equals(KUSTO_CLASS_NAME);
  }

  public static boolean isKustoSource(CreatableRelationProvider provider) {
    return provider.getClass().getName().equals(KUSTO_PROVIDER_CLASS_NAME);
  }

  public static boolean hasKustoClasses() {
    try {
      log.info("Try #1");
      KustoRelationVisitor.class
          .getClassLoader()
          .loadClass("com.microsoft.kusto.spark.datasource.DefaultSource");
      return true;
    } catch (Exception e) {
      // swallow
    }
    try {
      log.info("Try #2");
      Thread.currentThread()
          .getContextClassLoader()
          .loadClass("com.microsoft.kusto.spark.datasource.DefaultSource");
      return true;
    } catch (Exception e) {
      // swallow
    }

    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isKustoClass(plan);
  }

  private static Optional<String> getName(BaseRelation relation) {
    String tableName = "";
    try {
      Object query = FieldUtils.readField(relation, "query", true);
      tableName = (String) query;
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover Kusto table property");
      return Optional.empty();
    }

    if (tableName == "") {
      log.warn("Unable to discover Kusto table property");
      return Optional.empty();
    }
    // Check if the query is complex
    int n = tableName.length();
    int countPipes = 0;
    for (int i = 0; i < n; i++) {
      if (tableName.charAt(i) == '|') {
        countPipes++;
      }
    }
    if (countPipes > 0) {
      tableName = "COMPLEX";
    }
    return Optional.of(tableName);
  } // end of getName

  private static Optional<String> getNameSpace(BaseRelation relation) {
    String url;
    String databaseName;
    String url_prefix;
    try {
      Object kustoCoords = FieldUtils.readField(relation, "kustoCoordinates", true);
      Object clusterUrl = FieldUtils.readField(kustoCoords, "clusterUrl", true);
      Object database = FieldUtils.readField(kustoCoords, "database", true);

      url_prefix = (String) clusterUrl;
      databaseName = (String) database;
      url = KUSTO_PREFIX + url_prefix + "/" + databaseName;

    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover clusterUrl or database property");
      return Optional.empty();
    }
    if (url == "") {
      return Optional.empty();
    }

    return Optional.of(url);
  } // end of getNameSpace

  public static <D extends OpenLineage.Dataset> List<D> createKustoDatasets(
      DatasetFactory<D> datasetFactory,
      scala.collection.immutable.Map<String, String> options,
      StructType schema) {
    List<D> output;

    Map<String, String> javaOptions =
        io.openlineage.spark.agent.util.ScalaConversionUtils.fromMap(options);

    String name = javaOptions.get("kustotable");
    String database = javaOptions.get("kustodatabase");
    String kustoCluster = javaOptions.get("kustocluster");

    String namespace =
        KUSTO_PREFIX + KUSTO_URL_PREFIX + kustoCluster + KUSTO_URL_SUFFIX + "/" + database;
    output = Collections.singletonList(datasetFactory.getDataset(name, namespace, schema));
    return output;
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    BaseRelation relation = ((LogicalRelation) x).relation();
    List<D> output;
    Optional<String> name = getName(relation);
    Optional<String> namespace = getNameSpace(relation);
    if (name.isPresent() && namespace.isPresent()) {
      output =
          Collections.singletonList(
              factory.getDataset(name.get(), namespace.get(), relation.schema()));
    } else {
      output = Collections.emptyList();
    }
    return output;
  }
} // end of KustoRelationVisitor
