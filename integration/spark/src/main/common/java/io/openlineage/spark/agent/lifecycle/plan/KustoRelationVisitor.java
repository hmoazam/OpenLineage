package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;

/** */
@Slf4j
public class KustoRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {

  private final DatasetFactory<D> factory;
  private static final String KUSTO_CLASS_NAME =
      "com.microsoft.kusto.spark.datasource.KustoRelation";

  private static final String KUSTO_CLASS_NAME_2 =
      "com.microsoft.kusto.spark.datasource.DefaultSource";

  public KustoRelationVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  public static boolean hasKustoClasses() {
    // try {
    //   // KustoRelationVisitor.class.getClassLoader().loadClass(KUSTO_CLASS_NAME);
    //   // log.error(
    //   // "haskusto classes check output class name 2: ",
    //   // KustoRelationVisitor.class.getClassLoader().loadClass(KUSTO_CLASS_NAME_2));
    //   // log.error(
    //   //     "haskusto classes check output class name 1: ",
    //   // KustoRelationVisitor.class.getClassLoader().loadClass(KUSTO_CLASS_NAME));
    //   return true;
    // } catch (Exception e) {
    //   // swallow - we don't care
    //   log.error("haskusto classes check threw exception: ", e);
    // }

    return true;
  }

  public static boolean isKustoClass(LogicalPlan plan) {
    return plan instanceof LogicalRelation
        && ((LogicalRelation) plan).relation().getClass().getName().equals(KUSTO_CLASS_NAME);
  }

  public static boolean isKustoSource(CreatableRelationProvider provider) {
    if (!hasKustoClasses()) {
      return false;
    }
    log.error("Kusto provider class name: " + provider.getClass().getName());
    return provider.getClass().getName().equals("com.microsoft.kusto.spark.datasink.DefaultSource");
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return isKustoClass(plan);
  }

  // val fieldDetails = FieldUtils.readField(sqlBaseRelation,"query",true)
  // val kustoCoords = FieldUtils.readField(sqlBaseRelation,"kustoCoordinates",true) //class
  // KustoCoordinates(clusterUrl: String, clusterAlias: String, database: String, table:
  // Option[String] = None)
  // val clusterUrl = FieldUtils.readField(kustoCoords,"clusterUrl",true)
  // val clusterAlias = FieldUtils.readField(kustoCoords,"clusterAlias",true)
  // val table = FieldUtils.readField(kustoCoords,"table",true)

  private Optional<String> getName(BaseRelation relation) {
    String tableName = "";
    try {

      // Object fieldDetails = FieldUtils.readField(relation, "query", true);
      Object kustoCoords = FieldUtils.readField(relation, "kustoCoordinates", true);
      // Object clusterUrl = FieldUtils.readField(kustoCoords, "clusterUrl", true);
      // Object clusterAlias = FieldUtils.readField(kustoCoords, "clusterAlias", true);
      Object table = FieldUtils.readField(kustoCoords, "table", true);
      tableName = (String) table;
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover Kusto table property");
      return Optional.empty();
    }
    if (tableName == "") {
      log.warn("Unable to discover Kusto table property");
      return Optional.empty();
    }
    // TODO: see what to do with complex queries which come under fieldDetails
    return Optional.of(tableName);
  } // end of getName

  private Optional<String> getNameSpace(BaseRelation relation) {
    String url;
    try {
      // Object fieldDetails = FieldUtils.readField(relation, "query", true);
      Object kustoCoords = FieldUtils.readField(relation, "kustoCoordinates", true);
      Object clusterUrl = FieldUtils.readField(kustoCoords, "clusterUrl", true);
      url = (String) clusterUrl;
    } catch (IllegalAccessException | IllegalArgumentException e) {
      log.warn("Unable to discover clusterUrl property");
      return Optional.empty();
    }
    if (url == "") {
      return Optional.empty();
    }
    return Optional.of(url);
  } // end of getNameSpace

  @Override
  public List<D> apply(LogicalPlan x) {
    BaseRelation relation = ((LogicalRelation) x).relation();
    List<D> output;
    Optional<String> name = getName(relation);
    Optional<String> namespace = getNameSpace(relation);
    if (name.isPresent() && namespace.isPresent()) {
      output =
          Collections.singletonList(
              factory.getDataset(
                  name.get(),
                  namespace.get(),
                  relation.schema())); // TODO: check what we need to put in the schema option
    } else {
      output = Collections.emptyList();
    }
    return output;
  }
} // end of KustoRelationVisitor
