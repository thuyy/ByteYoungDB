#pragma once

#include "sql/statements.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unordered_map>
#include <vector>

using namespace hsql;

namespace bydb {

#define TUPLE_GROUP_SIZE 100

typedef unsigned char uchar;

struct Tuple {
  Tuple* prev;
  Tuple* next;
  uchar data[];
};

class TupleList {
public:
  TupleList() {
    head_ = static_cast<Tuple *>(malloc(sizeof(Tuple)));
    tail_ = static_cast<Tuple *>(malloc(sizeof(Tuple)));
    head_->next = tail_;
    tail_->prev = head_;
    head_->prev = nullptr;
    tail_->next = nullptr;
  }

  void addHead(Tuple* tup) {
    Tuple* ntup = head_->next;
    ntup->prev = tup;
    tup->next = ntup;
    head_->next = tup;
    tup->prev = head_;
  }

  void delTuple(Tuple* tup) {
    Tuple* ntup = tup->next;
    Tuple* ptup = tup->prev;
    ptup->next = ntup;
    ntup->prev = ptup;
    tup->next = nullptr;
    tup->prev = nullptr;
  }

  Tuple* popHead() {
    if (head_->next == tail_) {
      return nullptr;
    }

    Tuple* tup = head_->next;
    delTuple(tup);
    return tup;
  }

  Tuple* getHead() {
    if (head_->next == tail_) {
      return nullptr;
    }
    return head_->next;
  }

  Tuple* getNext(Tuple* tup) {
    return (tup->next == tail_) ? nullptr : tup->next;
  }

  bool isEmpty() {
    return (head_->next == tail_);
  }

private:
  Tuple* head_;
  Tuple* tail_;
};

class TableStore {
 public:
  TableStore(std::vector<ColumnDefinition*>* columns);
  ~TableStore();

  bool insertTuple(std::vector<Expr*>* values);
  bool deleteTuple(Tuple* tup);
  bool updateTuple(Tuple* tup, std::vector<UpdateClause*>* updates);

  Tuple* seqScan(Tuple* tup);
  void parseTuple(Tuple* tup, std::vector<Expr*>& values);

 private:
  bool newTupleGroup();
  void setColValue(Tuple* tup, int idx, Expr* expr);

  int colNum_;
  int tupleSize_;
  uint64_t rowCount_;

  std::vector<ColumnDefinition*>* columns_;
  std::vector<int> colOffset_;
  std::vector<Tuple*> tupleGroups_;
  TupleList freeList_;
  TupleList dataList_;
};

}  // namespace bydb