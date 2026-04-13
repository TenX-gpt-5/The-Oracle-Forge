from __future__ import annotations

import importlib.util
import inspect
import sys
from pathlib import Path


def run() -> int:
    root = Path.cwd()
    tests_dir = root / "tests"
    test_files = sorted(tests_dir.glob("test_*.py"))
    failures = 0
    executed = 0

    for test_file in test_files:
        module_name = f"_local_{test_file.stem}"
        spec = importlib.util.spec_from_file_location(module_name, test_file)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)

        for name, fn in inspect.getmembers(module, inspect.isfunction):
            if not name.startswith("test_"):
                continue
            executed += 1
            try:
                fn()
                sys.stdout.write(".")
            except Exception as exc:
                failures += 1
                sys.stdout.write("F")
                sys.stdout.write(f"\n{name} failed: {exc}\n")

    sys.stdout.write("\n")
    if failures:
        sys.stdout.write(f"{failures} failed, {executed - failures} passed\n")
        return 1
    sys.stdout.write(f"{executed} passed\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
