import inspect
import importlib
from typing import Any, Callable, cast, get_type_hints

from temporalio.exceptions import ApplicationError

from .streaming import StreamContext

ActivityFn = Callable[..., Any]


async def call_activity(
    fn: ActivityFn, args: dict[str, Any], stream: StreamContext
) -> Any:
    result = fn(**_kwargs_for_activity(fn, stream, args))
    if inspect.isawaitable(result):
        return await result
    return result


def function_ref(fn: ActivityFn) -> str:
    module_name = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)
    if not module_name or not qualname or "<locals>" in qualname:
        raise ValueError(
            f"Activity function {fn} must be an importable module-level function"
        )

    return f"{module_name}:{qualname}"


def resolve_function_ref(function_ref: str) -> ActivityFn:
    module_name, separator, qualname = function_ref.partition(":")
    if not separator or not module_name or not qualname:
        raise ApplicationError(
            f"Invalid tool activity function reference: {function_ref}",
            type="InvalidToolActivityFunctionRef",
            non_retryable=True,
        )

    try:
        obj: Any = importlib.import_module(module_name)
        for attr in qualname.split("."):
            obj = getattr(obj, attr)
    except (ImportError, AttributeError) as err:
        raise ApplicationError(
            f"Unable to resolve tool activity function: {function_ref}",
            type="UnknownToolActivityFunction",
            non_retryable=True,
        ) from err

    if not callable(obj):
        raise ApplicationError(
            f"Tool activity function reference is not callable: {function_ref}",
            type="InvalidToolActivityFunctionRef",
            non_retryable=True,
        )

    return cast(ActivityFn, obj)


def _kwargs_for_activity(
    fn: ActivityFn, stream: StreamContext, args: dict[str, Any]
) -> dict[str, Any]:
    signature = inspect.signature(fn)
    type_hints = get_type_hints(fn)
    kwargs: dict[str, Any] = {}
    consumed_args: set[str] = set()

    for name, parameter in signature.parameters.items():
        if name in ("self", "cls"):
            continue

        annotation = type_hints.get(name, parameter.annotation)
        if annotation is StreamContext:
            kwargs[name] = stream
            continue

        if name in args:
            kwargs[name] = args[name]
            consumed_args.add(name)
        elif parameter.default is inspect.Parameter.empty:
            raise TypeError(f"Missing required activity argument {fn.__name__}.{name}")

    unexpected_args = set(args) - consumed_args
    if unexpected_args:
        names = ", ".join(sorted(unexpected_args))
        raise TypeError(
            f"Unexpected activity argument(s) for {fn.__name__}: {names}"
        )

    return kwargs
