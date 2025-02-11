def prepare_statement(statement: str) -> str:
    return statement.replace('\t', '').strip().strip('\n')
