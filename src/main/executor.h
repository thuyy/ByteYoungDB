namespace bydb {
class Executor {
}

class BaseOperator {
}

class InsertOperator : public BaseOperator {
}

class DeleteOperator : public BaseOperator {
}

class ScanOperator : public BaseOperator {
}

class IndexScanOperator : ScanOperator {
}

class SeqScanOperator : ScanOperator {
}

}  // namespace bydb