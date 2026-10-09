"""Client-side tools: Claude asks for them (a `tool_use` block), our server runs them.

These exist so you can watch the full tool round trip in the stream:
`tool_use` block -> `input_json_delta` fragments -> stop_reason "tool_use"
-> we execute -> `tool_result` goes back in the next request.
"""

import ast
import operator
from datetime import datetime
from typing import Any

TOOLS: list[dict[str, Any]] = [
    {
        "name": "calculator",
        "description": (
            "Evaluate an arithmetic expression exactly. Supports + - * / // % ** and parentheses. "
            "Use this for any non-trivial arithmetic instead of computing it yourself."
        ),
        # Stream the tool input as it is generated instead of in one burst at the end.
        "eager_input_streaming": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "expression": {"type": "string", "description": "e.g. '(17 ** 3) / 4'"},
            },
            "required": ["expression"],
        },
    },
    {
        "name": "get_current_time",
        "description": "Get the current date and time of the server, with its UTC offset.",
        "eager_input_streaming": True,
        "input_schema": {"type": "object", "properties": {}},
    },
]

_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def _safe_eval(node: ast.AST) -> float | int:
    """Evaluate a numeric expression AST without `eval()` (model output is untrusted)."""
    match node:
        case ast.Expression(body=body):
            return _safe_eval(body)
        case ast.Constant(value=int() | float() as value) if not isinstance(value, bool):
            return value
        case ast.BinOp(left=left, op=op, right=right) if type(op) in _OPERATORS:
            lhs, rhs = _safe_eval(left), _safe_eval(right)
            if isinstance(op, ast.Pow) and abs(rhs) > 1000:
                raise ValueError("exponent too large")
            return _OPERATORS[type(op)](lhs, rhs)
        case ast.UnaryOp(op=op, operand=operand) if type(op) in _OPERATORS:
            return _OPERATORS[type(op)](_safe_eval(operand))
    raise ValueError(f"unsupported expression element: {ast.dump(node)[:60]}")


def run_tool(name: str, tool_input: Any) -> tuple[str, bool]:
    """Run a tool. Returns (content, is_error).

    With eager input streaming the API does not validate the input against the
    schema, so we validate it ourselves before running anything.
    """
    if not isinstance(tool_input, dict):
        return f"Invalid input: expected an object, got {type(tool_input).__name__}", True
    try:
        if name == "calculator":
            expression = tool_input.get("expression")
            if not isinstance(expression, str) or not expression.strip():
                return "Invalid input: 'expression' must be a non-empty string", True
            result = _safe_eval(ast.parse(expression, mode="eval"))
            return str(result), False
        if name == "get_current_time":
            return datetime.now().astimezone().isoformat(timespec="seconds"), False
        return f"Unknown tool: {name}", True
    except (ValueError, SyntaxError, ZeroDivisionError, OverflowError) as e:
        return f"Error: {e}", True
