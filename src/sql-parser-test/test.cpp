#include "SQLParser.h"
#include "SQLParserResult.h"
#include "util/sqlhelper.h"

#include <stdlib.h>
#include <iostream>
#include <string>

using namespace std;

int main(int argc, char* argv[]) {
  std::string query = argv[1];

  // parse a given query
  hsql::SQLParserResult result;
  hsql::SQLParser::parse(query, &result);

  if (result.isValid()) {
    printf("Parsed successfully!\n");
    printf("Number of statements: %lu\n", result.size());

    for (auto i = 0u; i < result.size(); ++i) {
      // Print a statement summary.
      hsql::printStatementInfo(result.getStatement(i));
    }
    return 0;
  } else {
    fprintf(stderr, "Given string is not a valid SQL query.\n");
    fprintf(stderr, "%s (L%d:%d)\n", result.errorMsg(), result.errorLine(),
            result.errorColumn());
    return -1;
  }

  return 0;
}