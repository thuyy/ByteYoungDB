#pragma once

#include "storage.h"

#include <stack>

namespace bydb {
enum UndoType { kInsertUndo, kDeleteUndo, kUpdateUndo };

struct Undo {
  Undo(UndoType t)
      : type(t), tableStore(nullptr), curTup(nullptr), oldTup(nullptr) {}
  ~Undo() {
    if (type == kUpdateUndo) {
      free(oldTup);
    }
  }

  UndoType type;
  TableStore* tableStore;
  Tuple* curTup;
  Tuple* oldTup;
};

class Transaction {
 public:
  Transaction() : inTransaction_(false) {}
  ~Transaction() {}

  void addInsertUndo(TableStore* table_store, Tuple* tup);
  void addDeleteUndo(TableStore* table_store, Tuple* tup);
  void addUpdateUndo(TableStore* table_store, Tuple* tup);

  void begin();
  void rollback();
  void commit();

  bool inTransaction() { return inTransaction_; }

 private:
  bool inTransaction_;
  std::stack<Undo*> undoStack_;
};

extern Transaction g_transaction;
}  // namespace bydb