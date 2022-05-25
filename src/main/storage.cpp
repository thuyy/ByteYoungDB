#include "storage.h"
#include "util.h"

#include "sql/ColumnType.h"

#include <cstdint>
#include <cstring>
#include <iostream>

using namespace hsql;

namespace bydb {

TableStore::TableStore(std::vector<ColumnDefinition*>* columns)
    : colNum_(columns->size()), tupleSize_(0), rowCount_(0), columns_(columns) {
  colOffset_.push_back(0);

  // Add space for each columns
  for (auto col : *columns) {
    tupleSize_ += ColumnTypeSize(col->type);
    colOffset_.push_back(tupleSize_);
  }

  // Add space for null map
  tupleSize_ += colNum_;

  // Add space for header
  tupleSize_ += sizeof(bool);
  tupleSize_ += sizeof(trx_id_t);
}

TableStore::~TableStore() {
  for (auto tuple_group : tupleGroups_) {
    free(tuple_group);
  }
}

bool TableStore::insertTuple(std::vector<Expr*>* values) {
  if (freeTuples_.size() == 0) {
    if (newTupleGroup()) {
      return true;
    }
  }

  Tuple* tup = freeTuples_.back();
  freeTuples_.pop_back();

  tup->is_free = false;
  tup->trx_id = 0;

  bool* is_null = reinterpret_cast<bool*>(&tup->data[0]);
  uchar* data = tup->data + colNum_;

  int i = 0;
  for (auto expr : *values) {
    int offset = colOffset_[i];
    int size = colOffset_[i + 1] - colOffset_[i];
    uchar* ptr = &data[offset];
    is_null[i] = false;

    switch (expr->type) {
      case kExprLiteralInt: {
        if (size == 4) {
          *reinterpret_cast<int32_t*>(ptr) = static_cast<int>(expr->ival);
        } else {
          *reinterpret_cast<int64_t*>(ptr) = expr->ival;
        }
        break;
      }
      case kExprLiteralFloat: {
        if (size == 4) {
          *reinterpret_cast<float*>(ptr) = static_cast<float>(expr->fval);
        } else {
          *reinterpret_cast<double*>(ptr) = expr->fval;
        }
        break;
      }
      case kExprLiteralString: {
        int len = strlen(expr->name);
        memcpy(ptr, expr->name, len);
        ptr[len] = '\0';
        break;
      }
      case kExprLiteralNull:
        is_null[i] = true;
        break;
      default:
        break;
    }

    i++;
  }

  data_.push_back(tup);
  rowCount_++;
  return false;
}

bool TableStore::newTupleGroup() {
  Tuple* tuple_group =
      static_cast<Tuple*>(malloc(tupleSize_ * TUPLE_GROUP_SIZE));
  if (tuple_group == nullptr) {
    std::cout << "# ERROR: Failed to malloc " << tupleSize_ * TUPLE_GROUP_SIZE
              << " bytes";
    return true;
  }

  tupleGroups_.push_back(tuple_group);
  uchar* ptr = reinterpret_cast<uchar*>(tuple_group);
  for (int i = 0; i < TUPLE_GROUP_SIZE; i++) {
    Tuple* tup = reinterpret_cast<Tuple*>(ptr);
    freeTuples_.push_back(tup);
    ptr += tupleSize_;
  }

  return false;
}

}  // namespace bydb