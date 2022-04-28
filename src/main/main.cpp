#include "parser.h"

#include <stdlib.h>
#include <iostream>
#include <string>

using namespace bydb;
using namespace hsql;

static bool ExecStmt(std::string stmt) {
  Parser parser;
  if (parser.parseStatement(stmt)) {
    return true;
  }

  return false;
}

int main(int argc, char* argv[]) {
  std::cout << "# Welcome to ByteYoung DB!!!" << std::endl;
  std::cout << "# Input your query in one line." << std::endl;
  std::cout << "# Enter 'exit' to quit this program." << std::endl;

  std::string cmd;
  while (true) {
    std::getline(std::cin, cmd);
    if (cmd == "exit") {
      break;
    }

    if (ExecStmt(cmd)) {
      std::cout << "# ERROR: Failed to execute '" << cmd << "'" << std::endl;
    }
  }

  std::cout << "# Farewell~~~ " << std::endl;
  return 0;
}