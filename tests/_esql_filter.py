"""A small ES|QL ``WHERE`` evaluator over rebuilt documents (see ``tests._emit_harness``).

Three-valued: anything it cannot read (an unknown function, a missing field in a
comparison) evaluates to ``None``. Callers choose how to treat ``None`` — the alert
identity test keeps such rows, the workflow evidence test requires ``True``.
"""
from __future__ import annotations

import fnmatch
import json
import re

from tests._emit_harness import field as _field

_TOKEN = re.compile(
    r"""\s*(?:(?P<num>-?\d+(?:\.\d+)?)|(?P<str>"(?:[^"\\]|\\.)*")|(?P<op>==|!=|>=|<=|>|<|\(|\)|,)"""
    r"""|(?P<name>`[^`]+`|[A-Za-z_@][\w.@]*(?:\.`[^`]+`)*))"""
)


def _tokens(text: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    while pos < len(text):
        match = _TOKEN.match(text, pos)
        if not match or match.end() == pos:
            if text[pos:].strip():
                out.append(("unknown", text[pos]))
            pos += 1
            continue
        pos = match.end()
        kind = match.lastgroup
        out.append((kind, match.group(kind)))
    return out


class Filter:
    """A small ES|QL WHERE evaluator; anything it cannot read evaluates to ``None``."""

    def __init__(self, text: str):
        self.tokens = _tokens(text)
        self.pos = 0

    def _peek(self, upper: bool = True):
        if self.pos >= len(self.tokens):
            return None
        kind, value = self.tokens[self.pos]
        return value.upper() if upper and kind == "name" else value

    def _next(self):
        self.pos += 1
        return self.tokens[self.pos - 1]

    def evaluate(self, doc: dict):
        self.pos = 0
        try:
            result = self._or(doc)
        except (IndexError, ValueError, TypeError):
            return None
        return result if self.pos >= len(self.tokens) else None

    def _or(self, doc):
        values = [self._and(doc)]
        while self._peek() == "OR":
            self._next()
            values.append(self._and(doc))
        return True if True in values else (None if None in values else False)

    def _and(self, doc):
        values = [self._not(doc)]
        while self._peek() == "AND":
            self._next()
            values.append(self._not(doc))
        return False if False in values else (None if None in values else True)

    def _not(self, doc):
        if self._peek() == "NOT":
            self._next()
            value = self._not(doc)
            return None if value is None else not value
        return self._comparison(doc)

    def _comparison(self, doc):
        left = self._operand(doc)
        nxt = self._peek()
        if nxt == "IS":
            self._next()
            negate = self._peek() == "NOT"
            if negate:
                self._next()
            self._next()  # NULL
            return (left is not None) if negate else (left is None)
        negate = nxt == "NOT"
        if negate:
            self._next()
            nxt = self._peek()
        if nxt in ("LIKE", "RLIKE"):
            self._next()
            pattern = self._operand(doc)
            if left is None or pattern is None:
                return None
            if nxt == "LIKE":
                hit = fnmatch.fnmatchcase(str(left), str(pattern).replace("?", "[?]"))
            else:
                hit = re.fullmatch(str(pattern), str(left)) is not None
            return hit != negate
        if nxt == "IN":
            self._next()
            self._next()  # (
            options = [self._operand(doc)]
            while self._peek() == ",":
                self._next()
                options.append(self._operand(doc))
            self._next()  # )
            return None if left is None else (left in options) != negate
        if nxt in ("==", "!=", ">", ">=", "<", "<="):
            op = self._next()[1]
            right = self._operand(doc)
            if left is None or right is None:
                return None
            if isinstance(left, (int, float)) != isinstance(right, (int, float)):
                try:
                    left, right = float(left), float(right)
                except (TypeError, ValueError):
                    return None
            return {"==": left == right, "!=": left != right, ">": left > right,
                    ">=": left >= right, "<": left < right, "<=": left <= right}[op]
        return left if isinstance(left, bool) or left is None else None

    def _operand(self, doc):
        kind, value = self._next()
        if kind == "num":
            return float(value)
        if kind == "str":
            return json.loads(value)
        if kind == "op" and value == "(":
            inner = self._or(doc)
            self._next()  # )
            return inner
        if kind != "name":
            raise ValueError(value)
        if self._peek() == "(":
            return self._call(value.upper(), doc)
        if value.upper() in ("TRUE", "FALSE"):
            return value.upper() == "TRUE"
        if value.upper() == "NULL":
            return None
        found = _field(doc, value)
        return float(found) if isinstance(found, int) and not isinstance(found, bool) else found

    def _call(self, func: str, doc):
        self._next()  # (
        args = []
        if self._peek() != ")":
            args.append(self._or_operand(doc))
            while self._peek() == ",":
                self._next()
                args.append(self._or_operand(doc))
        self._next()  # )
        if func == "COALESCE":
            return next((a for a in args if a is not None), None)
        if func in ("TO_LOWER", "TO_UPPER") and isinstance(args[0], str):
            return args[0].lower() if func == "TO_LOWER" else args[0].upper()
        if func in ("TO_DOUBLE", "TO_LONG", "TO_INTEGER") and args[0] is not None:
            return float(args[0])
        raise ValueError(func)

    def _or_operand(self, doc):
        start = self.pos
        try:
            return self._operand(doc)
        except ValueError:
            self.pos = start
            raise
