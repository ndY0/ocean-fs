# Import all plugins so their @register decorators run at import time.
# Add new language plugins here.

from .rust import RustPlugin
from .java import JavaPlugin
from .python import PythonPlugin

__all__ = ["RustPlugin", "JavaPlugin", "PythonPlugin"]