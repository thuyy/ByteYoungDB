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
      return createTrxPlanTree(
          static_cast<const TransactionStatement*>(stmt));
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
  return plan;
}

Plan* Optimizer::createDropPlanTree(const DropStatement* stmt) {
  DropPlan* plan = new DropPlan();
  return plan;
}

Plan* Optimizer::createInsertPlanTree(const InsertStatement* stmt) {
  InsertPlan* plan = new InsertPlan();
  return plan;
}

Plan* Optimizer::createUpdatePlanTree(const UpdateStatement* stmt) {
  UpdatePlan* plan = new UpdatePlan();
  return plan;
}

Plan* Optimizer::createDeletePlanTree(const DeleteStatement* stmt) {
  DeletePlan* plan = new DeletePlan();
  return plan;
}

Plan* Optimizer::createSelectPlanTree(const SelectStatement* stmt) {
  SelectPlan* plan = new SelectPlan();
  return plan;
}

Plan* Optimizer::createTrxPlanTree(const TransactionStatement* stmt) {
  TrxPlan* plan = new TrxPlan();
  return plan;
}

Plan* Optimizer::createShowPlanTree(const ShowStatement* stmt) {
  ShowPlan* plan = new ShowPlan();
  return plan;
}

Plan* Optimizer::createScanPlan() {
  ScanPlan* plan = new ScanPlan();
  return plan;
}

Plan* Optimizer::createFileterPlan() {
  FilterPlan* plan = new FilterPlan();
  return plan;
}

Plan* Optimizer::createProjdctionPlan() {
  ProjectionPlan* plan = new ProjectionPlan();
  return plan;
}

Plan* Optimizer::createSortPlan() {
  SortPlan* plan = new SortPlan();
  return plan;
}

Plan* Optimizer::createLimitPlan() {
  LimitPlan* plan = new LimitPlan();
  return plan;
}

}  // namespace bydb