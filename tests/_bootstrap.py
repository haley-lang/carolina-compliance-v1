"""Bootstrap: install mock modules for packages that may not be available."""
import sys
import types

def _ensure_module(name, source_module=None):
    """Install a mock module into sys.modules if the real one can't import."""
    if name in sys.modules:
        return
    try:
        __import__(name)
    except ImportError:
        if source_module:
            sys.modules[name] = source_module
        else:
            sys.modules[name] = types.ModuleType(name)

# Try importing real packages first; fall back to stubs
try:
    import pyairtable
except ImportError:
    from tests._stubs import pyairtable_stub
    sys.modules['pyairtable'] = pyairtable_stub

try:
    import anthropic
except ImportError:
    from tests._stubs import anthropic_stub
    sys.modules['anthropic'] = anthropic_stub

# Ensure dotenv is available (stub if needed)
try:
    import dotenv
except ImportError:
    dotenv_mod = types.ModuleType('dotenv')
    dotenv_mod.load_dotenv = lambda *a, **kw: None
    sys.modules['dotenv'] = dotenv_mod

# Ensure pytz is available
try:
    import pytz
except ImportError:
    import datetime
    pytz_mod = types.ModuleType('pytz')
    class _FakeTZ:
        def localize(self, dt): return dt
    pytz_mod.timezone = lambda name: _FakeTZ()
    sys.modules['pytz'] = pytz_mod

# Ensure pytest is available
try:
    import pytest
except ImportError:
    from tests._stubs import pytest_shim
    sys.modules['pytest'] = pytest_shim
