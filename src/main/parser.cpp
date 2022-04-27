#include "metadata.h"
#include "parser.h"
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
  for (auto i = 0; i < result_->size(); ++i) {
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

bool Parser::checkInsertStmt(const InsertStatement* insert_stmt) { return false; }

bool Parser::checkUpdateStmt(const UpdateStatement* update_stmt) { return false; }

bool Parser::checkDeleteStmt(const DeleteStatement* delete_stmt) { return false; }

bool Parser::checkCreateStmt(const CreateStatement* create_stmt) {
  switch (create_stmt->type) {
    case kCreateTable:
      if (checkCreateTableStmt(create_stmt)) {
        return true;
      }
      break;
    case kCreateIndex:
      if (checkCreateIndexStmt(create_stmt)) {
        return true;
      }
      break;
    default:
      std::cout << "# ERROR: Only support 'Create Table/Index'." << std::endl;
      return true;
  }

  return false;
}

bool Parser::checkDropStmt(const DropStatement* drop_stmt) {
  switch (drop_stmt->type) {
    case kDropTable:

    case kDropSchema:

    case kDropIndex:

    default:
      std::cout << "# ERROR: Not support drop statement "
                << DropTypeToString(drop_stmt->type) << std::endl;
      return true;
  }

  return false;
}

bool Parser::checkTable(TableRef* table) {
  if (table->type != kTableName) {
    std::cout << "# ERROR: Only support ordinary table." << std::endl;
    return true;
  }

  if (table->schema == nullptr || table->name == nullptr) {
    std::cout
        << "ERROR: Schema and table name should be specified in the query."
        << std::endl;
    return true;
  }

  TableName table_name;
  SetTableName(table_name, table->schema, table->name);
  if (g_meta_data.getTable(table_name) == nullptr) {
    return true;
  }

  return false;
}

bool Parser::checkCreateTableStmt(const CreateStatement* create_stmt) {
  TableName table_name;
  SetTableName(table_name, create_stmt->schema, create_stmt->tableName);

  if (g_meta_data.getTable(table_name) != nullptr &&
      !create_stmt->ifNotExists) {
    std::cout << "# ERROR: Table " << TableNameToString(table_name)
              << " already existed!" << std::endl;
    return true;
  }

  if (create_stmt->columns == nullptr || create_stmt->columns->size() == 0) {
    std::cout << "# ERROR: Valid column should be spicified in 'Create "
                  "Table' statement."
              << std::endl;
    return true;
  }

  for (auto col_def : *create_stmt->columns) {
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

bool Parser::checkCreateIndexStmt(const CreateStatement* create_stmt) {
  TableName table_name;
  SetTableName(table_name, create_stmt->schema, create_stmt->tableName);

  Table* table = g_meta_data.getTable(table_name);
  if (table == nullptr) {
    std::cout << "# ERROR: Table " << TableNameToString(table_name)
              << " did not exist!" << std::endl;
    return true;
  }

  for (auto idx : table->indexes) {
    if (strcmp(idx->name, create_stmt->indexName) == 0 &&
        !create_stmt->ifNotExists) {
      std::cout << "# ERROR: Index " << idx->name << " already existed!"
                << std::endl;
      return true;
    }
  }

  for (auto idx_col : *create_stmt->indexColumns) {
    bool find_col = false;
    for (auto col_def : table->columns) {
      if (strcmp(idx_col, col_def->name) == 0) {
        find_col = true;
      }
    }
    if (!find_col) {
      std::cout << "# ERROR: Can not find column " << idx_col << " for index "
                << create_stmt->indexName << std::endl;
      return true;
    }
  }

  return false;
}

}  // namespace bydb