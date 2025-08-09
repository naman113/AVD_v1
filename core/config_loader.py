import threading
import time
import yaml
from pathlib import Path
from typing import Any, Dict, Callable

class ConfigLoader:
    def __init__(self, path: str, reload_seconds: int = 15):
        self.path = Path(path)
        self.reload_seconds = reload_seconds
        self._data: Dict[str, Any] = {}
        self._callbacks: list[Callable[[Dict[str, Any]], None]] = []
        self._mtime = 0.0
        self._lock = threading.RLock()
        self.load()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()

    def load(self):
        with self._lock:
            if not self.path.exists():
                raise FileNotFoundError(self.path)
            data = yaml.safe_load(self.path.read_text()) or {}
            self._data = data
            self._mtime = self.path.stat().st_mtime

    def get(self) -> Dict[str, Any]:
        with self._lock:
            return self._data.copy()

    def on_change(self, cb: Callable[[Dict[str, Any]], None]):
        self._callbacks.append(cb)

    def _watch(self):
        while True:
            try:
                time.sleep(self.reload_seconds)
                m = self.path.stat().st_mtime
                if m != self._mtime:
                    self.load()
                    for cb in self._callbacks:
                        try:
                            cb(self.get())
                        except Exception:
                            pass
            except Exception:
                time.sleep(self.reload_seconds)
