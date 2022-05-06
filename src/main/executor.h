#include "optimizer.h"

namespace bydb {
class BaseOperator {
 public:
  virtual bool exec() = 0;
  Plan* plan_;
  BaseOperator* next_;
};

class CreateOperator : public BaseOperator {
 public:
  bool exec() override;
};

class DropOperator : public BaseOperator {};

class InsertOperator : public BaseOperator {};

class UpdateOperator : public BaseOperator {};

class SelectOperator : public BaseOperator {};

class TrxOperator : public BaseOperator {};

class ShowOperator : public BaseOperator {
 public:
  bool exec() override;
};

class DeleteOperator : public BaseOperator {};

class ScanOperator : public BaseOperator {};

class IndexScanOperator : ScanOperator {};

class SeqScanOperator : ScanOperator {};

class FilterOperator : BaseOperator {};

class SortOperator : BaseOperator {};

class LimitOperator : BaseOperator {};

class ProjectionOperator : BaseOperator {};

class Executor {
 public:
  Executor(Plan* plan) : planTree_(plan) {}
  ~Executor();
  bool generateOperator();

 private:
  Plan* planTree_;
  BaseOperator* opTree_;
};

}  // namespace bydb