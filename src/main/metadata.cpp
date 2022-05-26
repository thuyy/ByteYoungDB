#include "metadata.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

MetaData g_meta_data;

Table::Table(char* schema, char* name, std::vector<ColumnDefinition*>* columns) {
  schema_ = strdup(schema);
  name_ = strdup(name);
  for (auto col_old : *columns) {
    std::vector<ConstraintType>* column_constraints =
        new std::vector<ConstraintType>();
    *column_constraints = *col_old->column_constraints;
    ColumnDefinition* col = new ColumnDefinition(
        strdup(col_old->name), col_old->type, column_constraints);
    col->nullable = col_old->nullable;
    columns_.push_back(col);
  }

  tableStore_ = new TableStore(&columns_);
}

Table::~Table() {
  free(schema_);
  free(name_);
  delete tableStore_;
  for (auto col : columns_) {
    delete col;
  }
}

ColumnDefinition* Table::getColumn(char* name) {
  if (name == nullptr || strlen(name) == 0) {
    return nullptr;
  }

  for (auto col : columns_) {
    if (strcmp(name, col->name) == 0) {
      return col;
    }
  }

  return nullptr;
}

Index* Table::getIndex(char* name) {
  if (name == nullptr || strlen(name) == 0) {
    return nullptr;
  }

  for (auto index : indexes_) {
    if (strcmp(name, index->name)) {
      return index;
    }
  }

  return nullptr;
}

bool MetaData::insertTable(Table* table) {
  if (getTable(table->schema(), table->name()) != nullptr) {
    return true;
  } else {
    TableName table_name;
    SetTableName(table_name, table->schema(), table->name());
    table_map_.emplace(table_name, table);
    return false;
  }
}

bool MetaData::dropIndex(char* schema, char* name, char* indexName) {
  Table* table = getTable(schema, name);
  if (table == nullptr) {
    std::cout << "[BYDB-Error]  Table " << TableNameToString(schema, name)
              << " did not exist!" << std::endl;
    return true;
  }

  bool ret = true;
  std::vector<Index*>& indexes = *table->indexes();
  for (size_t i = 0; i < indexes.size(); i++) {
    Index* index = indexes[i];
    if (strcmp(index->name, indexName) == 0) {
      indexes.erase(indexes.begin() + i);
      ret = false;
    }
  }

  return ret;
}

bool MetaData::dropTable(char* schema, char* name) {
  Table* table = getTable(schema, name);
  if (table == nullptr) {
    return true;
  }

  TableName table_name;
  SetTableName(table_name, schema, name);
  table_map_.erase(table_name);
  delete table;
  return false;
}

bool MetaData::dropSchema(char* schema) {
  auto iter = table_map_.begin();
  bool ret = true;
  while (iter != table_map_.end()) {
    Table* table = iter->second;
    if (strcmp(table->schema(), schema) == 0) {
      std::cout << "[BYDB-Info]  Drop table " << table->name() << " in schema "
                << schema << std::endl;
      iter = table_map_.erase(iter);
      delete table;
      ret = false;
    } else {
      iter++;
    }
  }
  return ret;
}

void MetaData::getAllTables(std::vector<Table*>* tables) {
  if (tables == nullptr) {
    return;
  }

  for (auto iter : table_map_) {
    tables->push_back(iter.second);
  }
}

bool MetaData::findSchema(char* schema) {
  for (auto iter : table_map_) {
    Table* table = iter.second;
    if (strcmp(table->schema(), schema) == 0) {
      return true;
    }
  }

  return false;
}

Table* MetaData::getTable(char* schema, char* name) {
  if (schema == nullptr || name == nullptr) {
    std::cout
        << "ERROR: Schema and table name should be specified in the query."
        << std::endl;
    return nullptr;
  }

  TableName table_name;
  SetTableName(table_name, schema, name);

  auto iter = table_map_.find(table_name);
  if (iter == table_map_.end()) {
    return nullptr;
  } else {
    return iter->second;
  }
}

Index* MetaData::getIndex(char* schema, char* name, char* index_name) {
  Table* table = getTable(schema, name);
  if (table == nullptr) {
    std::cout << "[BYDB-Error]  Table " << TableNameToString(schema, name)
              << " did not exist!" << std::endl;
    return nullptr;
  }

  for (auto index : *table->indexes()) {
    if (strcmp(index->name, index_name) == 0) {
      return index;
    }
  }

  return nullptr;
}

}  // namespace bydb