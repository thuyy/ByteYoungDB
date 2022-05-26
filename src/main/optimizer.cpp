#include "optimizer.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

Plan* Optimizer::createPlanTree(const SQLStatement* stmt) {
  switch (stmt->type()) {
    case kStmtSelect:
      return createSelectPlanTree(static_cast<const SelectStatement*>(stmt));
    case kStmtInsert:
      return createInsertPlanTree(static_cast<const InsertStatement*>(stmt));
    case kStmtUpdate:
      return createUpdatePlanTree(static_cast<const UpdateStatement*>(stmt));
    case kStmtDelete:
      return createDeletePlanTree(static_cast<const DeleteStatement*>(stmt));
    case kStmtCreate:
      return createCreatePlanTree(static_cast<const CreateStatement*>(stmt));
    case kStmtDrop:
      return createDropPlanTree(static_cast<const DropStatement*>(stmt));
    case kStmtTransaction:
      return createTrxPlanTree(static_cast<const TransactionStatement*>(stmt));
    case kStmtShow:
      return createShowPlanTree(static_cast<const ShowStatement*>(stmt));
    default:
      std::cout << "# ERROR: Statement type " << StmtTypeToString(stmt->type())
                << " is not supported now." << std::endl;
  }
  return nullptr;
}

Plan* Optimizer::createCreatePlanTree(const CreateStatement* stmt) {
  CreatePlan* plan = new CreatePlan(stmt->type);
  plan->ifNotExists = stmt->ifNotExists;
  plan->type = stmt->type;
  plan->schema = stmt->schema;
  plan->tableName = stmt->tableName;
  plan->indexName = stmt->indexName;
  plan->columns = stmt->columns;
  plan->next = nullptr;

  if (plan->type == kCreateIndex) {
    Table* table = g_meta_data.getTable(plan->schema, plan->tableName);
    if (table == nullptr) {
      delete plan;
      return nullptr;
    }

    if (stmt->indexColumns != nullptr) {
      plan->indexColumns = new std::vector<ColumnDefinition*>;
    }

    for (auto col_name : *stmt->indexColumns) {
      ColumnDefinition* col_def = table->getColumn(col_name);
      if (col_def == nullptr) {
        delete plan->indexColumns;
        delete plan;
        return nullptr;
      }
      plan->indexColumns->push_back(col_def);
    }
  }

  return plan;
}

Plan* Optimizer::createDropPlanTree(const DropStatement* stmt) {
  DropPlan* plan = new DropPlan();
  plan->type = stmt->type;
  plan->ifExists = stmt->ifExists;
  plan->schema = stmt->schema;
  plan->name = stmt->name;
  plan->indexName = stmt->indexName;
  plan->next = nullptr;
  return plan;
}

Plan* Optimizer::createInsertPlanTree(const InsertStatement* stmt) {
  InsertPlan* plan = new InsertPlan();
  plan->type = stmt->type;
  plan->table = g_meta_data.getTable(stmt->schema, stmt->tableName);
  plan->values = stmt->values;

  return plan;
}

Plan* Optimizer::createUpdatePlanTree(const UpdateStatement* stmt) {
  Table* table = g_meta_data.getTable(stmt->table->schema, stmt->table->name);
  Plan* plan;

  ScanPlan* scan = new ScanPlan();
  scan->type = kSeqScan;
  scan->table = table;
  plan = scan;

  if (stmt->where != nullptr) {
    Plan* filter = createFilterPlan(table->columns(), stmt->where);
    filter->next = plan;
    plan = filter;
  }

  UpdatePlan* update = new UpdatePlan();
  update->table = table;
  update->next = plan;

  for (auto upd : *stmt->updates) {
    size_t idx = 0;
    update->values.push_back(upd->value);
    for (auto col : *table->columns()) {
      if (strcmp(upd->column, col->name) == 0) {
        update->idxs.push_back(idx);
        break;
      }
      idx++;
    }
  }

  return update;
}

Plan* Optimizer::createDeletePlanTree(const DeleteStatement* stmt) {
  Table* table = g_meta_data.getTable(stmt->schema, stmt->tableName);
  Plan* plan;

  ScanPlan* scan = new ScanPlan();
  scan->type = kSeqScan;
  scan->table = table;
  plan = scan;

  if (stmt->expr != nullptr) {
    Plan* filter = createFilterPlan(table->columns(), stmt->expr);
    filter->next = plan;
    plan = filter;
  }

  DeletePlan* del = new DeletePlan();
  del->table = table;
  del->next = plan;
  return del;
}

Plan* Optimizer::createSelectPlanTree(const SelectStatement* stmt) {
  Table* table = g_meta_data.getTable(stmt->fromTable->schema, stmt->fromTable->name);
  std::vector<ColumnDefinition*>* columns = table->columns();
  Plan *plan;

  ScanPlan* scan = new ScanPlan();
  scan->type = kSeqScan;
  scan->table = table;
  plan = scan;

  if (stmt->whereClause != nullptr) {
    Plan* filter = createFilterPlan(columns, stmt->whereClause);
    filter->next = plan;
    plan = filter;
  }

  SelectPlan* select = new SelectPlan();
  select->table = table;
  select->next = plan;

  for (auto expr : *stmt->selectList) {
    if (expr->type == kExprStar) {
      for (size_t i = 0; i < columns->size(); i++) {
        ColumnDefinition* col = (*columns)[i];
        select->outCols.push_back(col);
        select->colIds.push_back(i);
      }
    } else {
      for (size_t i = 0; i < columns->size(); i++) {
        ColumnDefinition* col = (*columns)[i];
        if (strcmp(expr->name, col->name) == 0) {
          select->outCols.push_back(col);
          select->colIds.push_back(i);
        }
      }
    }
  }

  return select;
}

Plan* Optimizer::createFilterPlan(std::vector<ColumnDefinition*>* columns, Expr* where) {
  FilterPlan* filter = new FilterPlan();
  Expr* col = nullptr;
  Expr* val = nullptr;
  if (where->expr->type == kExprColumnRef) {
    col = where->expr;
    val = where->expr2;
  } else {
    col = where->expr2;
    val = where->expr;
  }

  for (size_t i = 0 ; i < columns->size(); i++) {
    ColumnDefinition* col_def = (*columns)[i];
    if (strcmp(col->name, col_def->name) == 0) {
      filter->idx = i;
    }
  }
  filter->val = val;

  return filter;
}

Plan* Optimizer::createTrxPlanTree(const TransactionStatement* stmt) {
  TrxPlan* plan = new TrxPlan();
  return plan;
}

Plan* Optimizer::createShowPlanTree(const ShowStatement* stmt) {
  ShowPlan* plan = new ShowPlan();
  plan->type = stmt->type;
  plan->schema = stmt->schema;
  plan->name = stmt->name;
  plan->next = nullptr;
  return plan;
}
}  // namespace bydb