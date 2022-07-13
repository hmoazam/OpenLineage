package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** */
@Slf4j
public class CosmosDBHandler implements CatalogHandler {
  public boolean hasClasses() {
    try {
      CosmosDBHandler.class.getClassLoader().loadClass("com.azure.cosmos.spark.CosmosCatalog");
      System.out.println("Successfully checked cosmos class");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog
        .getClass()
        .getCanonicalName()
        .equals("com.azure.cosmos.spark.CosmosCatalog");
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    // String namespace = "azurecosmos";
    // String relationName = relation.table().name().replace("com.azure.cosmos.spark.items.", "");
    // int expectedParts = 3;
    // String[] tableParts = relationName.split("\\.", expectedParts);
    // String tableName;
    // if (tableParts.length != expectedParts) {
    //     tableName = relationName;
    //     namespace = relationName;
    // } else {
    //     namespace =
    //         String.format(
    //             "azurecosmos://%s.documents.azure.com/dbs/%s", tableParts[0], tableParts[1]);
    //     tableName = String.format("/colls/%s", tableParts[2]);
    // }
    System.out.println("Inside getDatasetIdentifier");
    System.out.println(identifier.name());
    System.out.println(identifier.namespace());

    String namespace = tableCatalog.name();
    String name = identifier.toString();

    return new DatasetIdentifier(name, namespace);
  }

  public Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
    return Optional.of(new TableProviderFacet("cosmos", "json")); // Cosmos is always json
  }

  @Override
  public String getName() {
    return "cosmos";
  }
}
