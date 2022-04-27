#pragma once
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "util/sqlhelper.h"

using namespace hsql;

namespace bydb {
class Parser {
 public:
  Parser();
  ~Parser();

  bool parseStatement(std::string query);

 private:
  bool checkStmtsMeta();

  bool checkMeta(const SQLStatement* stmt);

  bool checkSelectStmt(const SelectStatement* select_stmt);

  bool checkInsertStmt(const InsertStatement* insert_stmt);

  bool checkUpdateStmt(const UpdateStatement* update_stmt);

  bool checkDeleteStmt(const DeleteStatement* delete_stmt);

  bool checkCreateStmt(const CreateStatement* create_stmt);

  bool checkDropStmt(const DropStatement* drop_stmt);

  bool checkCreateIndexStmt(const CreateStatement* stmt);

  bool checkCreateTableStmt(const CreateStatement* stmt);

  bool checkTable(TableRef* table);

  SQLParserResult* result_;
};

}  // namespace bydb