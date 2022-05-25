#pragma once

#include "sql/CreateStatement.h"
#include "sql/Expr.h"

#include <cstdint>
#include <cstdlib>
#include <vector>

using namespace hsql;

namespace bydb {

#define TUPLE_GROUP_SIZE 100

typedef unsigned char uchar;
typedef uint64_t trx_id_t;

struct Tuple {
  trx_id_t trx_id;
  bool is_free;
  uchar data[];
};

class TableStore {
 public:
  TableStore(std::vector<ColumnDefinition*>* columns);
  ~TableStore();

  bool insertTuple(std::vector<Expr*>* values);
  bool deleteTuple(Tuple* tup);
  bool updateTuple();

  void seqScan();
  void indexScan();

 private:
  bool newTupleGroup();

  int colNum_;
  int tupleSize_;
  uint64_t rowCount_;

  std::vector<ColumnDefinition*>* columns_;
  std::vector<int> colOffset_;
  std::vector<Tuple*> tupleGroups_;
  std::vector<Tuple*> freeTuples_;
  std::vector<Tuple*> data_;
};

}  // namespace bydb