#include "executor.h"
#include "metadata.h"
#include "optimizer.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

bool CreateOperator::exec() {
  CreatePlan* create_plan = static_cast<CreatePlan*>(plan_);

  if (create_plan->type == kCreateTable) {
    Table* table =
        g_meta_data.getTable(create_plan->schema, create_plan->tableName);
    if (table != nullptr) {
      if (create_plan->ifNotExists) {
        return false;
      } else {
        std::cout << "# ERROR: Table "
                  //<< TableNameTostring(create_plan->schema,
                  //                     create_plan->tableName)
                  << " already existed." << std::endl;
        return true;
      }
    }

    table = new Table(create_plan->schema, create_plan->tableName,
                      create_plan->columns);
    g_meta_data.insertTable(table);
  } else if (create_plan->type == kCreateIndex) {
    Table* table =
        g_meta_data.getTable(create_plan->schema, create_plan->tableName);
    if (table == nullptr) {
      std::cout << "# ERROR: Table "
                //<< TableNameTostring(create_plan->schema,
                //                     create_plan->tableName)
                << " did not exist." << std::endl;
      return true;
    }

    Index* index = table->getIndex(create_plan->indexName);
    if (index != nullptr) {
      if (create_plan->ifNotExists) {
        return false;
      } else {
        std::cout << "# ERROR: Index " << create_plan->indexName
                  << " already existed." << std::endl;
        return true;
      }
    }

    index = new Index();
    index->name = create_plan->indexName;
    index->columns = *create_plan->indexColumns;
    table->addIndex(index);
  } else {
    std::cout << "# ERROR: Invalid 'Show' statement." << std::endl;
    return true;
  }

  return false;
}

bool ShowOperator::exec() {
  ShowPlan* show_plan = static_cast<ShowPlan*>(plan_);
  if (show_plan->type == kShowTables) {
    std::vector<Table*> tables;
    g_meta_data.getAllTables(&tables);

    std::cout << "# Table List:" << std::endl;
    for (auto table : tables) {
      std::cout << "# " << TableNameToString(table->schema(),
                                             table->name())
                << std::endl;
    }
  } else if (show_plan->type == kShowColumns) {
    Table* table = g_meta_data.getTable(show_plan->schema, show_plan->name);
    if (table == nullptr) {
      std::cout << "# ERROR: Failed to find table "
                //<< TableNameTostring(show_plan->schema, show_plan->name)
                << std::endl;
      return true;
    }
    std::cout << "# " //<< TableNameTostring(show_plan->schema, show_plan->name)
              << " column list:" << std::endl;
    for (auto col_def : *table->columns()) {
      std::cout << "# " << col_def->name << std::endl;
    }
  } else {
    std::cout << "# ERROR: Invalid 'Show' statement." << std::endl;
    return true;
  }

  return false;
}

}  // namespace bydb