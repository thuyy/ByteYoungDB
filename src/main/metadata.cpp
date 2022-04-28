#include "metadata.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

MetaData g_meta_data;

bool MetaData::insertTable(Table* table) {
  if (getTable(table->name.schema, table->name.name) != nullptr) {
    std::cout << "# ERROR: Table " << TableNameToString(table->name)
              << " already existed!" << std::endl;
    return false;
  } else {
    table_map_.emplace(table->name, table);
    return true;
  }
}

bool MetaData::dropTable(char* schema, char* name) {
  Table* table = getTable(schema, name);
  if (table == nullptr) {
    std::cout << "# ERROR: Table " << TableNameToString(schema, name)
              << " did not exist!" << std::endl;
    return true;
  }

  table_map_.erase(table->name);
  delete table;
  return false;
}

void MetaData::dropSchema(char* schema) {
  auto iter = table_map_.begin();
  while (iter != table_map_.end()) {
    Table* table = iter->second;
    if (strcmp(table->name.schema, schema) == 0) {
      iter = table_map_.erase(iter);
      delete table;
    } else {
      iter++;
    }
  }
}

bool MetaData::findSchema(char* schema) {
  for (auto iter : table_map_) {
    Table* table = iter.second;
    if (strcmp(table->name.schema, schema) == 0) {
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
    std::cout << "# ERROR: Table " << TableNameToString(schema, name)
              << " did not exist!" << std::endl;
    return nullptr;
  }

  for (auto index : table->indexes) {
    if (strcmp(index->name, index_name) == 0) {
      return index;
    }
  }

  return nullptr;
}

}  // namespace bydb