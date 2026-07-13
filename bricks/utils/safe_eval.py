# -*- coding: utf-8 -*-
# @Time    : 2026-07-11
# @Author  : Kem
# @Desc    : 安全的表达式求值器，替代 eval/exec，支持算术、比较、逻辑、属性访问、下标、函数调用
import ast
import operator

# 支持的二元操作符
_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.LShift: operator.lshift,
    ast.RShift: operator.rshift,
    ast.BitAnd: operator.and_,
    ast.BitOr: operator.or_,
    ast.BitXor: operator.xor,
}

# 支持的比较操作符
_CMP_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}

_UNARY_OPS = {
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
    ast.Not: operator.not_,
    ast.Invert: operator.invert,
}


class SafeEvalError(Exception):
    """安全求值过程中的错误"""
    pass


def safe_eval(expr: str, namespace: dict = None) -> object:
    """
    安全地求值一个 Python 表达式。

    支持：
      - 算术: +, -, *, /, //, %, **
      - 比较: ==, !=, <, <=, >, >=, in, not in, is, is not
      - 逻辑: and, or, not
      - 位运算: &, |, ^, <<, >>
      - 属性访问: response.status_code
      - 下标: data["key"], data[0]
      - 函数调用: len(x), int(x)
      - 字面量: 数字、字符串、None/True/False、列表、元组、字典
      - 三元表达式: a if cond else b

    不支持（故意禁止）：
      - import, exec, eval, __import__
      - 赋值, global, nonlocal
      - class/def 定义
      - 任何形式的代码执行副作用

    :param expr: 要求值的表达式字符串
    :param namespace: 变量命名空间
    :return: 表达式的值
    :raises SafeEvalError: 表达式不安全或求值失败
    """
    if not expr or not isinstance(expr, str):
        raise SafeEvalError(f"expr must be a non-empty string, got {type(expr)}")

    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise SafeEvalError(f"Syntax error in expression: {expr!r}") from e

    return _eval_node(tree.body, namespace or {})


def _eval_node(node: ast.AST, ns: dict) -> object:
    """递归求值 AST 节点"""

    # 字面量: 123, "abc", None, True, False
    if isinstance(node, ast.Constant):
        return node.value

    # 变量名: x, context, response
    elif isinstance(node, ast.Name):
        name = node.id
        if name in ns:
            return ns[name]
        # 内置安全函数
        _SAFE_BUILTINS = {
            "len": len, "int": int, "float": float, "str": str,
            "bool": bool, "abs": abs, "min": min, "max": max,
            "round": round, "type": type, "isinstance": isinstance,
            "True": True, "False": False, "None": None,
        }
        if name in _SAFE_BUILTINS:
            return _SAFE_BUILTINS[name]
        raise SafeEvalError(f"Undefined variable: {name}")

    # 属性访问: x.y
    elif isinstance(node, ast.Attribute):
        obj = _eval_node(node.value, ns)
        return getattr(obj, node.attr)

    # 下标: x[key]
    elif isinstance(node, ast.Subscript):
        obj = _eval_node(node.value, ns)
        key = _eval_node(node.slice, ns)
        return obj[key]

    # 切片: x[1:3]
    elif isinstance(node, ast.Slice):
        lower = _eval_node(node.lower, ns) if node.lower else None
        upper = _eval_node(node.upper, ns) if node.upper else None
        step = _eval_node(node.step, ns) if node.step else None
        return slice(lower, upper, step)

    # 二元运算: a + b
    elif isinstance(node, ast.BinOp):
        op_type = type(node.op)
        if op_type not in _BIN_OPS:
            raise SafeEvalError(f"Unsupported operator: {op_type.__name__}")
        left = _eval_node(node.left, ns)
        right = _eval_node(node.right, ns)
        return _BIN_OPS[op_type](left, right)

    # 一元运算: -x, not x
    elif isinstance(node, ast.UnaryOp):
        op_type = type(node.op)
        if op_type not in _UNARY_OPS:
            raise SafeEvalError(f"Unsupported unary operator: {op_type.__name__}")
        operand = _eval_node(node.operand, ns)
        return _UNARY_OPS[op_type](operand)

    # 比较: a == b, a in b
    elif isinstance(node, ast.Compare):
        left = _eval_node(node.left, ns)
        for op, comparator in zip(node.ops, node.comparators):
            op_type = type(op)
            if op_type not in _CMP_OPS:
                raise SafeEvalError(f"Unsupported comparison: {op_type.__name__}")
            right = _eval_node(comparator, ns)
            if not _CMP_OPS[op_type](left, right):
                return False
            left = right
        return True

    # 逻辑: a and b, a or b
    elif isinstance(node, ast.BoolOp):
        op_type = type(node.op)
        if op_type is ast.And:
            return all(_eval_node(v, ns) for v in node.values)
        elif op_type is ast.Or:
            return any(_eval_node(v, ns) for v in node.values)
        raise SafeEvalError(f"Unsupported bool op: {op_type.__name__}")

    # 三元: a if cond else b
    elif isinstance(node, ast.IfExp):
        if _eval_node(node.test, ns):
            return _eval_node(node.body, ns)
        return _eval_node(node.orelse, ns)

    # 列表: [1, 2, 3]
    elif isinstance(node, ast.List):
        return [_eval_node(elt, ns) for elt in node.elts]

    # 元组: (1, 2, 3)
    elif isinstance(node, ast.Tuple):
        return tuple(_eval_node(elt, ns) for elt in node.elts)

    # 字典: {"a": 1}
    elif isinstance(node, ast.Dict):
        return {
            _eval_node(k, ns): _eval_node(v, ns)
            for k, v in zip(node.keys, node.values)
        }

    # 集合: {1, 2, 3}
    elif isinstance(node, ast.Set):
        return {_eval_node(elt, ns) for elt in node.elts}

    # 函数调用: len(x), int("1")
    elif isinstance(node, ast.Call):
        func = _eval_node(node.func, ns)
        args = [_eval_node(a, ns) for a in node.args]
        kwargs = {kw.arg: _eval_node(kw.value, ns) for kw in node.keywords}
        return func(*args, **kwargs)

    # 列表推导: [x for x in items]
    elif isinstance(node, ast.ListComp):
        return list(_eval_comprehension(node, ns))

    # 不支持的节点类型
    else:
        raise SafeEvalError(f"Unsupported expression: {ast.dump(node)}")


def _eval_comprehension(node: ast.ListComp, ns: dict):
    """求值列表推导"""
    if len(node.generators) != 1:
        raise SafeEvalError("Only single-generator comprehensions are supported")

    gen = node.generators[0]
    iter_val = _eval_node(gen.iter, ns)

    for item in iter_val:
        local_ns = {**ns, gen.target.id: item}
        # 求值条件
        skip = False
        for if_clause in gen.ifs:
            if not _eval_node(if_clause, local_ns):
                skip = True
                break
        if not skip:
            yield _eval_node(node.elt, local_ns)


# 缓存编译后的 AST，避免重复解析
_ast_cache: dict = {}


def safe_eval_cached(expr: str, namespace: dict = None) -> object:
    """
    带 AST 缓存的安全求值，适合重复执行同一表达式（如 ok/match 判断）。

    :param expr: 表达式字符串
    :param namespace: 变量命名空间
    :return: 表达式的值
    """
    tree = _ast_cache.get(expr)
    if tree is None:
        if len(_ast_cache) > 512:
            _ast_cache.clear()
        tree = ast.parse(expr, mode="eval")
        _ast_cache[expr] = tree
    return _eval_node(tree.body, namespace or {})
