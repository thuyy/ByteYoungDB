#include "executor.h"
#include "metadata.h"
#include "optimizer.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

void Executor::init() { opTree_ = generateOperator(planTree_); }

bool Executor::exec() { return opTree_->exec(); }

BaseOperator* Executor::generateOperator(Plan* plan) {
  BaseOperator* op = nullptr;
  BaseOperator* next = nullptr;

  /* Build Operator tree from the leaf. */
  if (plan->next != nullptr) {
    next = generateOperator(plan->next);
  }

  switch (plan->planType) {
    case kCreate:
      op = new CreateOperator(plan, next);
      break;
    case kDrop:
      op = new DropOperator(plan, next);
      break;
    case kInsert:
      op = new InsertOperator(plan, next);
      break;
    case kUpdate:
      op = new UpdateOperator(plan, next);
      break;
    case kDelete:
      op = new DeleteOperator(plan, next);
      break;
    case kSelect:
      op = new SelectOperator(plan, next);
      break;
    case kScan: {
      ScanPlan* scan_plan = static_cast<ScanPlan*>(plan);
      if (scan_plan->type == kSeqScan) {
        op = new SeqScanOperator(plan, next);
      } else if (scan_plan->type == kIndexScan) {
        op = new IndexScanOperator(plan, next);
      }
      break;
    }
    case kProjection:
      op = new ProjectionOperator(plan, next);
      break;
    case kFilter:
      op = new FilterOperator(plan, next);
      break;
    case kSort:
      op = new SortOperator(plan, next);
      break;
    case kLimit:
      op = new LimitOperator(plan, next);
      break;
    case kTrx:
      op = new TrxOperator(plan, next);
      break;
    case kShow:
      op = new ShowOperator(plan, next);
      break;
    default:
      break;
  }

  return op;
}

bool CreateOperator::exec() {
  CreatePlan* plan = static_cast<CreatePlan*>(plan_);

  if (plan->type == kCreateTable) {
    Table* table = new Table(plan->schema, plan->tableName, plan->columns);
    if (g_meta_data.insertTable(table)) {
      if (plan->ifNotExists) {
        std::cout << "# INFO: Table "
                  << TableNameToString(plan->schema, plan->tableName)
                  << " already existed." << std::endl;
        return false;
      } else {
        std::cout << "# ERROR: Table "
                  << TableNameToString(plan->schema, plan->tableName)
                  << " already existed." << std::endl;
        return true;
      }
      delete table;
    }

    std::cout << "# INFO: Create table successfully." << std::endl;
    return false;
  } else if (plan->type == kCreateIndex) {
    Table* table = g_meta_data.getTable(plan->schema, plan->tableName);
    if (table == nullptr) {
      std::cout << "# ERROR: Table "
                << TableNameToString(plan->schema, plan->tableName)
                << " did not exist." << std::endl;
      return true;
    }

    Index* index = table->getIndex(plan->indexName);
    if (index != nullptr) {
      if (plan->ifNotExists) {
        return false;
      } else {
        std::cout << "# ERROR: Index " << plan->indexName << " already existed."
                  << std::endl;
        return true;
      }
    }

    index = new Index();
    index->name = plan->indexName;
    index->columns = *plan->indexColumns;
    table->addIndex(index);
    std::cout << "# INFO: Create index successfully." << std::endl;
  } else {
    std::cout << "# ERROR: Invalid 'Show' statement." << std::endl;
    return true;
  }

  return false;
}

bool DropOperator::exec() {
  DropPlan* plan = static_cast<DropPlan*>(plan_);
  if (plan->type == kDropSchema) {
    if (g_meta_data.dropSchema(plan->schema)) {
      if (plan->ifExists) {
        std::cout << "# INFO: Schema " << plan->schema << " did not exist."
                  << std::endl;
        return false;
      } else {
        std::cout << "# ERROR: Schema " << plan->schema << " did not exist."
                  << std::endl;
        return true;
      }
    }

    std::cout << "# INFO: Drop schema successfully." << std::endl;
    return false;
  } else if (plan->type == kDropTable) {
    if (g_meta_data.dropTable(plan->schema, plan->name)) {
      if (plan->ifExists) {
        std::cout << "# INFO: Table "
                  << TableNameToString(plan->schema, plan->name)
                  << " did not exist." << std::endl;
        return false;
      } else {
        std::cout << "# ERROR: Table "
                  << TableNameToString(plan->schema, plan->name)
                  << " did not exist." << std::endl;
        return true;
      }
    }

    std::cout << "# INFO: Drop schema successfully." << std::endl;
    return false;
  } else if (plan->type == kDropIndex) {
    if (g_meta_data.dropIndex(plan->schema, plan->name, plan->indexName)) {
      if (plan->ifExists) {
        std::cout << "# INFO: Index " << plan->indexName << " did not exist."
                  << std::endl;
        return false;
      } else {
        std::cout << "# ERROR: Index " << plan->indexName << " did not exist."
                  << std::endl;
        return true;
      }
    }

    std::cout << "# INFO: Drop index successfully." << std::endl;
    return false;
  } else {
    std::cout << "# ERROR: Invalid 'Drop' statement." << std::endl;
    return true;
  }

  return false;
}

bool InsertOperator::exec() {
  InsertPlan* plan = static_cast<InsertPlan*>(plan_);
  TableStore* table_store = plan->table->getTableStore();
  return table_store->insertTuple(plan->values);
}

bool UpdateOperator::exec() { return false; }

bool DeleteOperator::exec() { return false; }

bool TrxOperator::exec() { return false; }

bool ShowOperator::exec() {
  ShowPlan* show_plan = static_cast<ShowPlan*>(plan_);
  if (show_plan->type == kShowTables) {
    std::vector<Table*> tables;
    g_meta_data.getAllTables(&tables);

    std::cout << "# Table List:" << std::endl;
    for (auto table : tables) {
      std::cout << TableNameToString(table->schema(), table->name())
                << std::endl;
    }
  } else if (show_plan->type == kShowColumns) {
    Table* table = g_meta_data.getTable(show_plan->schema, show_plan->name);
    if (table == nullptr) {
      std::cout << "# ERROR: Failed to find table "
                << TableNameToString(show_plan->schema, show_plan->name)
                << std::endl;
      return true;
    }
    std::cout << "# Columns in "
              << TableNameToString(show_plan->schema, show_plan->name) << ":"
              << std::endl;
    for (auto col_def : *table->columns()) {
      if (col_def->type.data_type == DataType::CHAR ||
          col_def->type.data_type == DataType::VARCHAR) {
        std::cout << col_def->name << "\t"
                  << DataTypeToString(col_def->type.data_type) << "("
                  << col_def->type.length << ")" << std::endl;
      } else {
        std::cout << col_def->name << "\t"
                  << DataTypeToString(col_def->type.data_type) << std::endl;
      }
    }
  } else {
    std::cout << "# ERROR: Invalid 'Show' statement." << std::endl;
    return true;
  }

  return false;
}

bool SelectOperator::exec() { return false; }

bool IndexScanOperator::exec() { return false; }

bool SeqScanOperator::exec() { return false; }

bool FilterOperator::exec() { return false; }

bool SortOperator::exec() { return false; }

bool LimitOperator::exec() { return false; }

bool ProjectionOperator::exec() { return false; }

}  // namespace bydb