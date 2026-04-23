"""Minimal pytest compatibility shim for running tests with unittest."""
import functools
import unittest

class _MarkDecorator:
    """Minimal mark support."""
    def __init__(self, name='', args=None, kwargs=None):
        self.name = name
        self.args = args or ()
        self.kwargs = kwargs or {}
    
    def __call__(self, func_or_class):
        if isinstance(func_or_class, type):
            func_or_class._pytest_mark = self.name
            return func_or_class
        func_or_class._pytest_mark = self.name
        return func_or_class
    
    def __getattr__(self, name):
        return _MarkDecorator(name)

class _MarkNamespace:
    def __getattr__(self, name):
        return _MarkDecorator(name)

mark = _MarkNamespace()

class _FixtureDecorator:
    def __call__(self, func=None, **kwargs):
        if func is None:
            return lambda f: self._apply(f, **kwargs)
        return self._apply(func)
    
    def _apply(self, func, **kwargs):
        func._is_fixture = True
        func._fixture_kwargs = kwargs
        return func

fixture = _FixtureDecorator()

def skip(reason=""):
    raise unittest.SkipTest(reason)

def raises(exception_type, **kwargs):
    return _RaisesContext(exception_type)

class _RaisesContext:
    def __init__(self, exception_type):
        self.exception_type = exception_type
        self.value = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            raise AssertionError(f"Expected {self.exception_type.__name__} but no exception was raised")
        if not issubclass(exc_type, self.exception_type):
            return False
        self.value = exc_val
        return True

def param(*args, **kwargs):
    return args[0] if len(args) == 1 else args

def parametrize(argnames, argvalues):
    """Minimal parametrize support."""
    def decorator(func):
        func._parametrize = (argnames, argvalues)
        return func
    return decorator

mark.parametrize = parametrize
