/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.ScalaConversionUtils.asJavaOptional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.kafka010.KafkaRelation;
import org.apache.spark.sql.kafka010.KafkaSourceProvider;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;


/**
 * 
 */
@Slf4j
public class KustoRelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalRelation, D> {

  private final DatasetFactory<D> datasetFactory;
  private static final String KUSTO_CLASS_NAME = "com.microsoft.kusto.spark.datasource.KustoRelation";
  
  public KustoRelationVisitor(OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    super(context);
    this.datasetFactory = datasetFactory;
  }

  public static boolean hasKustoClasses() {
    try {
      KustoRelationVisitor.class.getClassLoader().loadClass(KUSTO_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // we don't care because if we reach here we no longer care about Kusto? Since it doesn't exist?
    }
    return false;
  }

  public static boolean isKustoRelationClass(LogicalPlan plan) {
    return plan instanceof LogicalRelation &&
    ((LogicalRelation) plan).relation().getClass().getName().equals(KUSTO_CLASS_NAME);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    isKustoRelationClass(plan);
  }

  private Optional<String> getName(BaseRelation relation) {
    String tableName = "";
    try {
      FieldUtils.readField
    }
  }


















} // end of KustoRelationVisitor

