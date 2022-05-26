#include "executor.h"
#include "metadata.h"
#include "optimizer.h"
#include "trx.h"
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
      }
      break;
    }
    case kFilter:
      op = new FilterOperator(plan, next);
      break;
    case kTrx:
      op = new TrxOperator(plan, next);
      break;
    case kShow:
      op = new ShowOperator(plan, next);
      break;
    default:
      std::cout << "# ERROR: Not support plan node " << PlanTypeToString(plan->planType);
      break;
  }

  return op;
}

bool CreateOperator::exec(TupleIter** iter) {
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

bool DropOperator::exec(TupleIter** iter) {
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

bool InsertOperator::exec(TupleIter** iter) {
  InsertPlan* plan = static_cast<InsertPlan*>(plan_);
  TableStore* table_store = plan->table->getTableStore();
  if (table_store->insertTuple(plan->values)) {
    return true;
  }
  std::cout << "# INFO: Insert tuple successfully." << std::endl;
  return false;
}

bool UpdateOperator::exec(TupleIter** iter) {
  UpdatePlan* update = static_cast<UpdatePlan *>(plan_);
  Table *table = update->table;
  TableStore *table_store = table->getTableStore();
  int upd_cnt = 0;

  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) {
      return true;
    }

    if (tup_iter == nullptr) {
      break;
    } else {
      table_store->updateTuple(tup_iter->tup, update->idxs, update->values);
      upd_cnt++;
    }
  }


  std::cout << "# INFO: Update " << upd_cnt << " tuple successfully." << std::endl;
  return false;
}

bool DeleteOperator::exec(TupleIter** iter) {
  Table *table = static_cast<DeletePlan *>(plan_)->table;
  TableStore *table_store = table->getTableStore();
  int del_cnt = 0;

  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) {
      return true;
    }

    if (tup_iter == nullptr) {
      break;
    } else {
      table_store->deleteTuple(tup_iter->tup);
      del_cnt++;
    }
  }

  std::cout << "# INFO: Delete " << del_cnt << " tuple successfully." << std::endl;
  return false;
}

bool TrxOperator::exec(TupleIter** iter) {
  TrxPlan* plan = static_cast<TrxPlan*>(plan_);
  switch (plan->command) {
    case kBeginTransaction:
      g_transaction.begin();
      std::cout << "# INFO: Start transaction" << std::endl;
      break;
    case kCommitTransaction:
      g_transaction.commit();
      std::cout << "# INFO: Commit transaction" << std::endl;
      break;
    case kRollbackTransaction:
      g_transaction.rollback();
      std::cout << "# INFO: Rollback transaction" << std::endl;
      break;
    default:
      break;
  }

  return false;
}

bool ShowOperator::exec(TupleIter** iter) {
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

bool SelectOperator::exec(TupleIter** iter) {
  SelectPlan* plan = static_cast<SelectPlan*>(plan_);
  std::vector<std::vector<Expr*>> tuples;
  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) {
      return true;
    }

    if (tup_iter == nullptr) {
      break;
    } else {
      tuples.push_back(tup_iter->values);
    }
  }

  PrintTuples(plan->outCols, plan->colIds, tuples);
  return false;  
}

bool SeqScanOperator::exec(TupleIter** iter) {
  ScanPlan* plan = static_cast<ScanPlan*>(plan_);
  TableStore* table_store = plan->table->getTableStore();
  Tuple* tup = nullptr;

  if (finish) {
    return false;
  }

  if (nextTuple_ == nullptr) {
    tup = table_store->seqScan(nullptr);
  } else {
    tup = nextTuple_;
  }

  if (tup == nullptr) {
    *iter = nullptr;
    return false;
  }

  TupleIter* tup_iter = new TupleIter(tup);
  table_store->parseTuple(tup, tup_iter->values);
  tuples_.push_back(tup_iter);
  *iter = tup_iter;

  nextTuple_ = table_store->seqScan(tup);
  if (nextTuple_ == nullptr) {
    finish = true;
  }
  return false;
}

bool FilterOperator::exec(TupleIter** iter) {
  *iter = nullptr;
  while (true) {
    TupleIter* tup_iter = nullptr;
    if (next_->exec(&tup_iter)) {
      return true;
    }

    if (tup_iter == nullptr) {
      break;
    }

    if (execEqualExpr(tup_iter)) {
      *iter = tup_iter;
      break;
    }
  }
  
  return false;
}

bool FilterOperator::execEqualExpr(TupleIter* iter) {
  FilterPlan* filter = static_cast<FilterPlan*>(plan_);
  Expr* val = filter->val;
  size_t col_id = filter->idx;

  Expr* col_val = iter->values[col_id];
  if (col_val->type != val->type) {
    return false;
  }

  if (col_val->type == kExprLiteralInt) {
    return (col_val->ival == val->ival);
  }
  
  if (col_val->type == kExprLiteralString) {
    return (strcmp(col_val->name, val->name) == 0);
  }

  return false;
}

}  // namespace bydb