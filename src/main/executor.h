#include "optimizer.h"

namespace bydb {
class BaseOperator {
 public:
  BaseOperator(Plan* plan, BaseOperator* next) : plan_(plan), next_(next) {}
  ~BaseOperator() {}
  virtual bool exec() = 0;

  Plan* plan_;
  BaseOperator* next_;
};

class CreateOperator : public BaseOperator {
 public:
  CreateOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~CreateOperator() {}
  bool exec() override;
};

class DropOperator : public BaseOperator {
 public:
  DropOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~DropOperator() {}
  bool exec() override;
};

class InsertOperator : public BaseOperator {
 public:
  InsertOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~InsertOperator() {}
  bool exec() override;
};

class UpdateOperator : public BaseOperator {
 public:
  UpdateOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~UpdateOperator() {}
  bool exec() override;
};

class DeleteOperator : public BaseOperator {
 public:
  DeleteOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~DeleteOperator() {}
  bool exec() override;
};

class TrxOperator : public BaseOperator {
 public:
  TrxOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~TrxOperator() {}
  bool exec() override;
};

class ShowOperator : public BaseOperator {
 public:
  ShowOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~ShowOperator() {}
  bool exec() override;
};

class SelectOperator : public BaseOperator {
 public:
  SelectOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~SelectOperator() {}
  bool exec() override;
};

class IndexScanOperator : public BaseOperator {
 public:
  IndexScanOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~IndexScanOperator() {}
  bool exec() override;
};

class SeqScanOperator : public BaseOperator {
 public:
  SeqScanOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~SeqScanOperator() {}
  bool exec() override;
};

class FilterOperator : public BaseOperator {
 public:
  FilterOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~FilterOperator() {}
  bool exec() override;
};

class SortOperator : public BaseOperator {
 public:
  SortOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~SortOperator() {}
  bool exec() override;
};

class LimitOperator : public BaseOperator {
 public:
  LimitOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~LimitOperator() {}
  bool exec() override;
};

class ProjectionOperator : public BaseOperator {
 public:
  ProjectionOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~ProjectionOperator() {}
  bool exec() override;
};

class Executor {
 public:
  Executor(Plan* plan) : planTree_(plan) {}
  ~Executor() {}
  void init();
  bool exec();

 private:
  BaseOperator* generateOperator(Plan* Plan);

  Plan* planTree_;
  BaseOperator* opTree_;
};

}  // namespace bydb