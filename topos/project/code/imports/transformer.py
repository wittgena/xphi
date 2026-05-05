# topos.project.code.imports.transformer
import os
import sys
import difflib
import argparse
import libcst as cst
from typing import List, Dict, Any, Tuple, Callable
from pathlib import Path

def node_to_str(node: cst.CSTNode) -> str:
    return cst.Module([]).code_for_node(node)

class ImportTransformer(cst.CSTTransformer):
    def __init__(self, old: str, new: str):
        self.old = old
        self.new = new

    def _should_replace(self, module_name: str) -> bool:
        return module_name == self.old or module_name.startswith(self.old + ".")

    def _replace(self, module_name: str) -> str:
        return self.new + module_name[len(self.old):]

    def leave_ImportFrom(self, original_node, updated_node):
        if original_node.module is None:
            return updated_node

        module_name = node_to_str(original_node.module)
        if self._should_replace(module_name):
            new_module = self._replace(module_name)
            return updated_node.with_changes(
                module=cst.parse_expression(new_module)
            )
        return updated_node

    def leave_Import(self, original_node, updated_node):
        new_names = []
        for alias in original_node.names:
            name_str = node_to_str(alias.name)
            if self._should_replace(name_str):
                new_name = self._replace(name_str)
                new_alias = alias.with_changes(name=cst.parse_expression(new_name))
                new_names.append(new_alias)
            else:
                new_names.append(alias)
        return updated_node.with_changes(names=new_names)

