import ast
from pathlib import Path


class DataclassTableFinder(ast.NodeVisitor):
    def __init__(self):
        self.tables = []

    def visit_ClassDef(self, node):
        # Check if the class is a dataclass
        is_dataclass = any(isinstance(decorator, ast.Name) and decorator.id == 'dataclass'
                           for decorator in node.decorator_list)

        # Look for __table__ assignment in class body
        has_table_field = any(isinstance(n, ast.Assign) and
                              any(isinstance(t, ast.Name) and t.id == '__table__' for t in n.targets)
                              for n in node.body)

        # If both conditions are met, store the class name
        if is_dataclass and has_table_field:
            self.tables.append(node.name)

        # Continue visiting the rest of the AST
        self.generic_visit(node)


class Scanner:
    def __init__(self, start: Path):
        self._start = start

    def find_all(self):
        for f in self._start.glob('**/*.py'): # TODO: skip virtual environments
            yield from self._find_dataclasses_with_table(f)

    def _find_dataclasses_with_table(self, path: Path):
        # Parse the source code into an AST
        tree = ast.parse(path.read_text())

        # Create a finder instance and visit the parsed tree
        finder = DataclassTableFinder()
        finder.visit(tree)

        # Return the list of dataclasses with __table__ field
        return finder.tables

