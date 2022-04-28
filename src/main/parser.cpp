#include "parser.h"
#include "metadata.h"
#include "util.h"

#include <iostream>

using namespace hsql;

namespace bydb {

Parser::Parser() { result_ = nullptr; }

Parser::~Parser() {
  delete result_;
  result_ = nullptr;
}

bool Parser::parseStatement(std::string query) {
  result_ = new SQLParserResult;
  SQLParser::parse(query, result_);

  if (result_->isValid()) {
    return checkStmtsMeta();
  }

  return true;
}

bool Parser::checkStmtsMeta() {
  for (size_t i = 0; i < result_->size(); ++i) {
    const SQLStatement* stmt = result_->getStatement(i);
    if (checkMeta(stmt)) {
      return true;
    }
  }

  return false;
}

bool Parser::checkMeta(const SQLStatement* stmt) {
  switch (stmt->type()) {
    case kStmtSelect:
      return checkSelectStmt(static_cast<const SelectStatement*>(stmt));
    case kStmtInsert:
      return checkInsertStmt(static_cast<const InsertStatement*>(stmt));
    case kStmtUpdate:
      return checkUpdateStmt(static_cast<const UpdateStatement*>(stmt));
    case kStmtDelete:
      return checkDeleteStmt(static_cast<const DeleteStatement*>(stmt));
    case kStmtCreate:
      return checkCreateStmt(static_cast<const CreateStatement*>(stmt));
    case kStmtDrop:
      return checkDropStmt(static_cast<const DropStatement*>(stmt));
    case kStmtTransaction:
    case kStmtShow:
      return false;
    default:
      std::cout << "# ERROR: Statement type " << StmtTypeToString(stmt->type())
                << " is not supported now." << std::endl;
  }

  return true;
}

bool Parser::checkSelectStmt(const SelectStatement* select_stmt) {
  TableRef* table = select_stmt->fromTable;
  if (checkTable(table)) {
    return true;
  }

  return false;
}

bool Parser::checkInsertStmt(const InsertStatement* insert_stmt) {
  return false;
}

bool Parser::checkUpdateStmt(const UpdateStatement* update_stmt) {
  return false;
}

bool Parser::checkDeleteStmt(const DeleteStatement* delete_stmt) {
  return false;
}

bool Parser::checkTable(TableRef* table_ref) {
  if (table_ref->type != kTableName) {
    std::cout << "# ERROR: Only support ordinary table." << std::endl;
    return true;
  }

  return false;
}

bool Parser::checkCreateStmt(const CreateStatement* stmt) {
  switch (stmt->type) {
    case kCreateTable:
      if (checkCreateTableStmt(stmt)) {
        return true;
      }
      break;
    case kCreateIndex:
      if (checkCreateIndexStmt(stmt)) {
        return true;
      }
      break;
    default:
      std::cout << "# ERROR: Only support 'Create Table/Index'." << std::endl;
      return true;
  }

  return false;
}

bool Parser::checkCreateTableStmt(const CreateStatement* stmt) {
  // Check if the table already existed.
  if (g_meta_data.getTable(stmt->schema, stmt->tableName) != nullptr &&
      !stmt->ifNotExists) {
    std::cout << "# ERROR: Table "
              << TableNameToString(stmt->schema, stmt->tableName)
              << " already existed!" << std::endl;
    return true;
  }

  // Check each columns
  if (stmt->columns == nullptr || stmt->columns->size() == 0) {
    std::cout << "# ERROR: Valid column should be spicified in 'Create "
                 "Table' statement."
              << std::endl;
    return true;
  }

  for (auto col_def : *stmt->columns) {
    if (col_def == nullptr || col_def->name == nullptr) {
      std::cout << "# ERROR: Valid column should be spicified in 'Create "
                   "Table' statement."
                << std::endl;
      return true;
    }

    if (!IsDataTypeSupport(col_def->type.data_type)) {
      std::cout << "# ERROR: Unsupport data type "
                << DataTypeToString(col_def->type.data_type) << std::endl;
      return true;
    }
  }

  return false;
}

bool Parser::checkCreateIndexStmt(const CreateStatement* stmt) {
  if (g_meta_data.getIndex(stmt->schema, stmt->tableName, stmt->indexName) !=
          nullptr &&
      !stmt->ifNotExists) {
    std::cout << "# ERROR: Index " << stmt->indexName
              << "of " << TableNameToString(stmt->schema, stmt->tableName)
              << " already existed!"
              << std::endl;
    return true;
  }

  // Check if each column of this index existed.
  Table* table = g_meta_data.getTable(stmt->schema, stmt->tableName);
  for (auto idx_col : *stmt->indexColumns) {
    bool find_col = false;
    for (auto col_def : table->columns) {
      if (strcmp(idx_col, col_def->name) == 0) {
        find_col = true;
      }
    }
    if (!find_col) {
      std::cout << "# ERROR: Can not find column " << idx_col << " for index "
                << stmt->indexName << std::endl;
      return true;
    }
  }

  return false;
}

bool Parser::checkDropStmt(const DropStatement* stmt) {
  switch (stmt->type) {
    case kDropTable: {
      if (g_meta_data.getTable(stmt->schema, stmt->name) == nullptr &&
          !stmt->ifExists) {
        std::cout << "# ERROR: Table " << TableNameToString(stmt->schema, stmt->name)
                  << " did not exist!" << std::endl;
        return true;
      }
      break;
    }
    case kDropSchema: {
      if (!g_meta_data.findSchema(stmt->schema) && !stmt->ifExists) {
        std::cout << "# ERROR: Schema " << stmt->schema << " did not exist"
                  << std::endl;
        return true;
      }
      break;
    }
    case kDropIndex: {
      if (g_meta_data.getIndex(stmt->schema, stmt->name, stmt->indexName) ==
              nullptr &&
          !stmt->ifExists) {
        std::cout << "# ERROR: Index " << stmt->indexName
                  << " of " << TableNameToString(stmt->schema, stmt->name)
                  << " did not exist!" << std::endl;
        return true;
      }
      break;
    }
    default:
      std::cout << "# ERROR: Not support drop statement "
                << DropTypeToString(stmt->type) << std::endl;
      return true;
  }

  return false;
}

}  // namespace bydb
