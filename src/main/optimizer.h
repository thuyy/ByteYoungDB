#pragma once

#include "metadata.h"

#include "sql/statements.h"

using namespace hsql;

namespace bydb {
enum PlanType {
  kCreate,
  kDrop,
  kInsert,
  kUpdate,
  kDelete,
  kSelect,
  kScan,
  kProjection,
  kFilter,
  kSort,
  kLimit,
  kTrx,
  kShow
};

struct Plan {
  Plan(PlanType t) : planType(t), next(nullptr) {}
  ~Plan() {
    delete next;
    next = nullptr;
  }

  PlanType planType;
  Plan* next;
};

struct CreatePlan : public Plan {
  CreatePlan(CreateType t) : Plan(kCreate), type(t) {}
  CreateType type;
  bool ifNotExists;
  char* schema;
  char* tableName;
  char* indexName;
  std::vector<ColumnDefinition*>* indexColumns;
  std::vector<ColumnDefinition*>* columns;
};

struct DropPlan : public Plan {
  DropPlan() : Plan(kDrop) {}
  DropType type;
  bool ifExists;
  char* schema;
  char* name;
  char* indexName;
};

struct InsertPlan : public Plan {
  InsertPlan() : Plan(kInsert) {}
  InsertType type;
  Table* table;
  std::vector<Expr*>* values;
};

struct UpdatePlan : public Plan {
  UpdatePlan() : Plan(kUpdate) {}
  Table* table;
  std::vector<UpdateClause*>* updates;
};

struct DeletePlan : public Plan {
  DeletePlan() : Plan(kDelete) {}
  Table* table;
  Expr* whereClause;
};

struct SelectPlan : public Plan {
  SelectPlan() : Plan(kSelect) {}
  Table* table;
  std::vector<ColumnDefinition*> outCols;
  std::vector<size_t> colIds;
};

enum ScanType { kSeqScan, kIndexScan };

struct ScanPlan : public Plan {
  ScanPlan() : Plan(kScan) {}
  ScanType type;
  Table* table;
};

struct FilterPlan : public Plan {
  FilterPlan() : Plan(kFilter), idx(0), val(nullptr) {}
  size_t idx;
  Expr* val;
};

struct SortPlan : public Plan {
  SortPlan() : Plan(kSort) {}
  Table* table;
  std::vector<OrderDescription*>* order;
};

struct LimitPlan : public Plan {
  LimitPlan() : Plan(kLimit) {}
  uint64_t offset;
  uint64_t limit;
};

struct TrxPlan : public Plan {
  TrxPlan() : Plan(kTrx) {}
  TransactionCommand command;
};

struct ShowPlan : public Plan {
  ShowPlan() : Plan(kShow) {}
  ShowType type;
  char* schema;
  char* name;
};

class Optimizer {
 public:
  Optimizer() {}

  Plan* createPlanTree(const SQLStatement* stmt);

 private:
  Plan* createCreatePlanTree(const CreateStatement* stmt);

  Plan* createDropPlanTree(const DropStatement* stmt);

  Plan* createInsertPlanTree(const InsertStatement* stmt);

  Plan* createUpdatePlanTree(const UpdateStatement* stmt);

  Plan* createDeletePlanTree(const DeleteStatement* stmt);

  Plan* createSelectPlanTree(const SelectStatement* stmt);

  Plan* createTrxPlanTree(const TransactionStatement* stmt);

  Plan* createShowPlanTree(const ShowStatement* stmt);
};

}  // namespace bydb