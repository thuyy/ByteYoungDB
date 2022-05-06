#include "executor.h"
#include "metadata.h"
#include "optimizer.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

void Executor::init() {
  opTree_ = generateOperator(planTree_);
}

bool Executor::exec() {
  return opTree_->exec();
}

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

bool DropOperator::exec() {
  return false;
}

bool InsertOperator::exec() {
  return false;
}

bool UpdateOperator::exec() {
  return false;
}

bool DeleteOperator::exec() {
  return false;
}

bool TrxOperator::exec() {
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

bool SelectOperator::exec() {
  return false;
}

bool IndexScanOperator::exec() {
  return false;
}

bool SeqScanOperator::exec() {
  return false;
}

bool FilterOperator::exec() {
  return false;
}

bool SortOperator::exec() {
  return false;
}

bool LimitOperator::exec() {
  return false;
}

bool ProjectionOperator::exec() {
  return false;
}

}  // namespace bydb