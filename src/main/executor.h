#pragma once

#include "optimizer.h"

namespace bydb {
class BaseOperator {
 public:
  BaseOperator(Plan* plan, BaseOperator* next) : plan_(plan), next_(next) {}
  ~BaseOperator() {}
  virtual bool exec(std::vector<Expr*>* values = nullptr) = 0;

  Plan* plan_;
  BaseOperator* next_;
};

class CreateOperator : public BaseOperator {
 public:
  CreateOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~CreateOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class DropOperator : public BaseOperator {
 public:
  DropOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~DropOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class InsertOperator : public BaseOperator {
 public:
  InsertOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~InsertOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class UpdateOperator : public BaseOperator {
 public:
  UpdateOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~UpdateOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class DeleteOperator : public BaseOperator {
 public:
  DeleteOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~DeleteOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class TrxOperator : public BaseOperator {
 public:
  TrxOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~TrxOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class ShowOperator : public BaseOperator {
 public:
  ShowOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~ShowOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class SelectOperator : public BaseOperator {
 public:
  SelectOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~SelectOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class SeqScanOperator : public BaseOperator {
 public:
  SeqScanOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next), curTuple_(nullptr) {}
  ~SeqScanOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;

 private:
  Tuple* curTuple_;
};

class FilterOperator : public BaseOperator {
 public:
  FilterOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan, next) {}
  ~FilterOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
};

class ProjectionOperator : public BaseOperator {
 public:
  ProjectionOperator(Plan* plan, BaseOperator* next)
      : BaseOperator(plan, next) {}
  ~ProjectionOperator() {}
  bool exec(std::vector<Expr*>* values = nullptr) override;
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