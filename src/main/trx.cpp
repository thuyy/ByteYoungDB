#include "trx.h"

namespace bydb {
Transaction g_transaction;

void Transaction::addInsertUndo(TableStore* table_store, Tuple* tup) {
  Undo* undo = new Undo(kInsertUndo);
  undo->tableStore = table_store;
  undo->curTup = tup;
  undoStack_.push(undo);
}

void Transaction::addDeleteUndo(TableStore* table_store, Tuple* tup) {
  Undo* undo = new Undo(kDeleteUndo);
  undo->tableStore = table_store;
  undo->oldTup = tup;
  undoStack_.push(undo);
}

void Transaction::addUpdateUndo(TableStore* table_store, Tuple* tup) {
  Undo* undo = new Undo(kUpdateUndo);
  undo->tableStore = table_store;
  undo->oldTup = static_cast<Tuple*>(malloc(table_store->tupleSize()));
  memcpy(undo->oldTup->data, tup->data,
         table_store->tupleSize() - TUPLE_HEADER_SIZE);
  undo->curTup = tup;
  undoStack_.push(undo);
}

void Transaction::begin() { inTransaction_ = true; }

void Transaction::rollback() {
  while (!undoStack_.empty()) {
    auto undo = undoStack_.top();
    TableStore* table_store = undo->tableStore;
    undoStack_.pop();
    switch (undo->type) {
      case kInsertUndo:
        table_store->removeTuple(undo->curTup);
        break;
      case kDeleteUndo:
        table_store->recoverTuple(undo->oldTup);
        break;
      case kUpdateUndo:
        memcpy(undo->curTup->data, undo->oldTup->data,
               table_store->tupleSize() - TUPLE_HEADER_SIZE);
        break;
      default:
        break;
    }
    delete undo;
  }
  inTransaction_ = false;
}

void Transaction::commit() {
  while (undoStack_.empty()) {
    auto undo = undoStack_.top();
    TableStore* table_store = undo->tableStore;
    undoStack_.pop();
    if (undo->type == kDeleteUndo) {
      table_store->freeTuple(undo->oldTup);
    }
    delete undo;
  }
  inTransaction_ = false;
}

}  // namespace bydb