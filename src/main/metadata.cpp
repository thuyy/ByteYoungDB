#include "metadata.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

MetaData g_meta_data;

bool MetaData::insertTable(Table* table) {
  if (getTable(table->name) != nullptr) {
    std::cout << "# ERROR: Table " << TableNameToString(table->name)
              << " already existed!" << std::endl;
    return false;
  } else {
    table_map_.emplace(table->name, table);
    return true;
  }
}

bool MetaData::dropTable(TableName& table_name) {
  Table* table = getTable(table_name);
  if (table == nullptr) {
    std::cout << "# ERROR: Table " << TableNameToString(table_name)
              << " did not exist!" << std::endl;
    return true;
  }

  table_map_.erase(table_name);
  delete table;
  return false;
}

Table* MetaData::getTable(TableName& table_name) {
  auto iter = table_map_.find(table_name);
  if (iter == table_map_.end()) {
    return nullptr;
  } else {
    return iter->second;
  }
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

}  // namespace bydb