/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;


public interface CatalogHandler {
  boolean hasClasses();

  boolean isClass(TableCatalog tableCatalog);

  DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties
      );

// Cosmos
  default DatasetIdentifier getDatasetIdentifier(
    DataSourceV2Relation relation) {
      return 
    }

  boolean isClass(DataSourceV2Relation)

  default Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
    return Optional.empty();
  }

  /** Try to find string that uniquely identifies version of a dataset. */
  default Optional<String> getDatasetVersion(
      TableCatalog catalog, Identifier identifier, Map<String, String> properties) {
    return Optional.empty();
  }

  String getName();
}
