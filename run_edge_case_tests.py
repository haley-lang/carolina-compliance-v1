#!/usr/bin/env python3
"""
Edge Case Test Runner for Carolina Compliance Solutions COI Pipeline.

Runs the full edge-case suite and produces:
  - Console summary (total / passed / failed / skipped)
  - HTML report at tests/edge_case_report.html
  - JUnit XML at tests/edge_case_results.xml

Supports two modes:
  1. pytest (preferred) — if pytest is installed, uses it with full report features
  2. unittest (fallback) — uses stdlib unittest with XML output for environments
     where pytest isn't available

Usage:
    python run_edge_case_tests.py            # run all edge-case tests
    python run_edge_case_tests.py -k date    # filter by keyword (pytest only)
    python run_edge_case_tests.py -v         # verbose output
"""

import subprocess
import sys
import os
import time
import traceback
import unittest
import xml.etree.ElementTree as ET
from io import StringIO
from pathlib import Path
from datetime import datetime


ROOT_DIR = Path(__file__).parent
TESTS_DIR = ROOT_DIR / "tests"
REPORT_HTML = TESTS_DIR / "edge_case_report.html"
REPORT_XML = TESTS_DIR / "edge_case_results.xml"

# Edge-case test modules in execution order
TEST_MODULES = [
    "tests.test_edge_degraded_scan",
    "tests.test_edge_date_formats",
    "tests.test_edge_coverage_amounts",
    "tests.test_edge_acord_versions",
    "tests.test_edge_cancellation",
    "tests.test_edge_missing_fields",
]

TEST_MODULE_PATHS = [m.replace(".", "/") + ".py" for m in TEST_MODULES]

CATEGORY_MAP = {
    "test_edge_degraded_scan": "1. Degraded Scan Quality",
    "test_edge_date_formats": "2. Non-Standard Date Formats",
    "test_edge_coverage_amounts": "3. Borderline Coverage Amounts",
    "test_edge_acord_versions": "4. Non-Standard ACORD Versions",
    "test_edge_cancellation": "5. Mid-Term Cancellation Notices",
    "test_edge_missing_fields": "6. Missing Fields & 7. Duplicate Submissions",
}


# ── XML Result Writer ────────────────────────────────────────────────────────

class JUnitXMLResult(unittest.TestResult):
    """Custom TestResult that records results as JUnit XML."""

    def __init__(self, stream=None):
        super().__init__(stream=stream)
        self.test_results = []  # list of (testcase, status, message, duration)
        self._start_time = None

    def startTest(self, test):
        super().startTest(test)
        self._start_time = time.time()

    def addSuccess(self, test):
        super().addSuccess(test)
        duration = time.time() - self._start_time
        self.test_results.append((test, "pass", "", duration))

    def addFailure(self, test, err):
        super().addFailure(test, err)
        duration = time.time() - self._start_time
        msg = self._exc_info_to_string(err, test)
        self.test_results.append((test, "fail", msg, duration))

    def addError(self, test, err):
        super().addError(test, err)
        duration = time.time() - self._start_time
        msg = self._exc_info_to_string(err, test)
        self.test_results.append((test, "error", msg, duration))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        duration = time.time() - (self._start_time or time.time())
        self.test_results.append((test, "skip", reason, duration))

    def write_xml(self, path: Path):
        root = ET.Element("testsuites")
        suite = ET.SubElement(root, "testsuite",
                              name="edge_case_tests",
                              tests=str(self.testsRun),
                              failures=str(len(self.failures)),
                              errors=str(len(self.errors)),
                              skipped=str(len(self.skipped)))

        for test, status, message, duration in self.test_results:
            test_id = str(test)
            # Parse "test_name (module.Class)" format
            classname = test.__class__.__module__ + "." + test.__class__.__qualname__
            name = test._testMethodName

            tc = ET.SubElement(suite, "testcase",
                               classname=classname,
                               name=name,
                               time=f"{duration:.3f}")

            if status == "fail":
                f = ET.SubElement(tc, "failure", message=message[:200])
                f.text = message
            elif status == "error":
                e = ET.SubElement(tc, "error", message=message[:200])
                e.text = message
            elif status == "skip":
                ET.SubElement(tc, "skipped", message=message)

        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        tree.write(str(path), encoding="unicode", xml_declaration=True)


# ── HTML Report Builder ──────────────────────────────────────────────────────

def build_html_report(xml_path: Path, output_path: Path) -> None:
    """Parse JUnit XML results and generate a styled HTML report."""
    if not xml_path.exists():
        print(f"  [skip] No XML results at {xml_path}")
        return

    tree = ET.parse(xml_path)
    root = tree.getroot()

    suites = root.findall(".//testsuite") if root.tag != "testsuite" else [root]

    total = passed = failed = skipped = errored = 0
    rows = []

    for suite in suites:
        total += int(suite.get("tests", 0))
        failed += int(suite.get("failures", 0))
        errored += int(suite.get("errors", 0))
        skipped += int(suite.get("skipped", 0))

        for tc in suite.findall("testcase"):
            name = tc.get("name", "?")
            classname = tc.get("classname", "")
            # Extract module name from classname like "tests.test_edge_date_formats.TestClass"
            parts = classname.split(".")
            module = ""
            for p in parts:
                if p.startswith("test_edge_"):
                    module = p
                    break
            category = CATEGORY_MAP.get(module, module or classname)
            duration = float(tc.get("time", 0))

            failure = tc.find("failure")
            skip_el = tc.find("skipped")
            error = tc.find("error")

            if failure is not None:
                status = "FAIL"
                css = "fail"
                detail = (failure.get("message") or failure.text or "")[:200]
            elif error is not None:
                status = "ERROR"
                css = "fail"
                detail = (error.get("message") or error.text or "")[:200]
            elif skip_el is not None:
                status = "SKIP"
                css = "skip"
                detail = skip_el.get("message", "")[:200]
            else:
                status = "PASS"
                css = "pass"
                detail = ""

            rows.append((category, classname, name, status, css, f"{duration:.3f}s", detail))

    passed = total - failed - errored - skipped

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>COI Pipeline — Edge Case Test Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         margin: 2rem; background: #f8f9fa; color: #212529; }}
  h1 {{ color: #0d47a1; }}
  .summary {{ display: flex; gap: 1.5rem; margin: 1.5rem 0; flex-wrap: wrap; }}
  .summary .card {{ padding: 1rem 1.5rem; border-radius: 8px; min-width: 120px;
                    text-align: center; color: #fff; font-size: 1.1rem; }}
  .card.total {{ background: #37474f; }}
  .card.passed {{ background: #2e7d32; }}
  .card.failed {{ background: #c62828; }}
  .card.skipped {{ background: #f57f17; color: #212529; }}
  .card .num {{ font-size: 2rem; font-weight: bold; }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 1.5rem;
           background: #fff; border-radius: 8px; overflow: hidden;
           box-shadow: 0 1px 3px rgba(0,0,0,0.12); }}
  th {{ background: #0d47a1; color: #fff; text-align: left;
       padding: 0.75rem 1rem; font-size: 0.85rem; text-transform: uppercase; }}
  td {{ padding: 0.6rem 1rem; border-bottom: 1px solid #e0e0e0;
       font-size: 0.9rem; vertical-align: top; }}
  tr:hover {{ background: #f5f5f5; }}
  .badge {{ display: inline-block; padding: 2px 10px; border-radius: 4px;
            font-weight: 600; font-size: 0.8rem; text-transform: uppercase; }}
  .badge.pass {{ background: #e8f5e9; color: #2e7d32; }}
  .badge.fail {{ background: #ffebee; color: #c62828; }}
  .badge.skip {{ background: #fff8e1; color: #f57f17; }}
  .detail {{ color: #757575; font-size: 0.8rem; max-width: 400px;
             word-break: break-word; white-space: pre-wrap; }}
  footer {{ margin-top: 2rem; color: #9e9e9e; font-size: 0.8rem; }}
  .category-header {{ background: #e3f2fd; font-weight: 600; }}
</style>
</head>
<body>
<h1>COI Pipeline — Edge Case Test Report</h1>
<p>Generated: {now}</p>

<div class="summary">
  <div class="card total"><div class="num">{total}</div>Total</div>
  <div class="card passed"><div class="num">{passed}</div>Passed</div>
  <div class="card failed"><div class="num">{failed + errored}</div>Failed</div>
  <div class="card skipped"><div class="num">{skipped}</div>Skipped</div>
</div>

<table>
<thead>
<tr><th>Category</th><th>Class</th><th>Test</th><th>Result</th><th>Time</th><th>Detail</th></tr>
</thead>
<tbody>
"""
    for category, classname, name, status, css, duration, detail in rows:
        short_class = classname.rsplit(".", 1)[-1] if classname else ""
        escaped_detail = detail.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        html += f'<tr><td>{category}</td><td>{short_class}</td><td>{name}</td>'
        html += f'<td><span class="badge {css}">{status}</span></td>'
        html += f'<td>{duration}</td><td class="detail">{escaped_detail}</td></tr>\n'

    html += """</tbody>
</table>
<footer>Carolina Compliance Solutions — Automated Edge Case Testing Suite</footer>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
    print(f"  HTML report saved to: {output_path}")


# ── Runners ──────────────────────────────────────────────────────────────────

def run_with_pytest(extra_args):
    """Run tests using pytest (preferred)."""
    cmd = [
        sys.executable, "-m", "pytest",
        *TEST_MODULE_PATHS,
        f"--junitxml={REPORT_XML}",
        "--tb=short",
        "-q",
        *extra_args,
    ]
    print(f"  Runner: pytest")
    print(f"  Command: {' '.join(cmd)}\n")
    return subprocess.run(cmd).returncode


def _collect_pytest_style_tests(module):
    """Convert pytest-style test classes (no base class) into unittest.TestCase subclasses."""
    import importlib
    import inspect

    cases = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if not name.startswith("Test"):
            continue

        # If already a TestCase subclass, use as-is
        if issubclass(obj, unittest.TestCase):
            cases.append(obj)
            continue

        # Convert bare class to TestCase dynamically
        methods = {}
        for mname, mobj in inspect.getmembers(obj, inspect.isfunction):
            if mname.startswith("test_"):
                methods[mname] = mobj

        if not methods:
            continue

        # Check for fixtures (pytest-style)
        fixtures = {}
        for mname, mobj in inspect.getmembers(obj, inspect.isfunction):
            if hasattr(mobj, '_is_fixture') or (not mname.startswith("test_") and not mname.startswith("_")):
                fixtures[mname] = mobj

        # Create a new TestCase subclass
        tc_dict = {"__module__": module.__name__}

        for mname, mobj in methods.items():
            # Wrap the method to handle self and fixtures
            def make_test(method, fixture_dict):
                def test_method(self):
                    # Create an instance of the original class
                    import inspect as _insp
                    sig = _insp.signature(method)
                    params = list(sig.parameters.keys())

                    kwargs = {}
                    for p in params[1:]:  # skip self
                        if p == "tmp_path":
                            import tempfile
                            kwargs[p] = Path(tempfile.mkdtemp())
                        elif p in fixture_dict:
                            fixture_result = fixture_dict[p](None)
                            kwargs[p] = fixture_result

                    try:
                        method(self, **kwargs)
                    except unittest.SkipTest:
                        raise
                    except Exception as e:
                        if "skip" in str(type(e).__name__).lower():
                            raise unittest.SkipTest(str(e))
                        raise
                return test_method
            tc_dict[mname] = make_test(mobj, fixtures)

        tc_class = type(name, (unittest.TestCase,), tc_dict)
        tc_class.__module__ = module.__name__
        tc_class.__qualname__ = name
        cases.append(tc_class)

    return cases


def run_with_unittest():
    """Run tests using stdlib unittest (fallback)."""
    print(f"  Runner: unittest (pytest not available)\n")

    # Ensure bootstrap runs first
    sys.path.insert(0, str(ROOT_DIR))
    os.environ.setdefault("AIRTABLE_API_KEY", "test_fake_key")
    os.environ.setdefault("AIRTABLE_BASE_ID", "appTEST12345")
    os.environ.setdefault("ANTHROPIC_API_KEY", "test_fake_key")
    os.environ.setdefault("EMAIL_ADDRESS", "coi-intake@carolinacompliancesolutions.com")
    os.environ.setdefault("EMAIL_PASSWORD", "abcdefghijklmnop")

    import tests._bootstrap  # noqa: F401
    import importlib

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for module_name in TEST_MODULES:
        try:
            mod = importlib.import_module(module_name)
            test_classes = _collect_pytest_style_tests(mod)
            for tc_class in test_classes:
                suite.addTests(loader.loadTestsFromTestCase(tc_class))
            print(f"  Loaded {module_name} ({len(test_classes)} classes)")
        except Exception as exc:
            print(f"  [warn] Failed to load {module_name}: {exc}")
            traceback.print_exc()

    # Run with our custom result handler
    result = JUnitXMLResult()

    # Run the suite manually to use our custom result
    print()
    print("-" * 70)
    suite_start = time.time()
    suite(result)
    suite_duration = time.time() - suite_start
    print("-" * 70)

    # Write XML
    result.write_xml(REPORT_XML)

    # Print summary
    print(f"\n  Ran {result.testsRun} tests in {suite_duration:.2f}s")
    print(f"  Passed:  {result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped)}")
    print(f"  Failed:  {len(result.failures)}")
    print(f"  Errors:  {len(result.errors)}")
    print(f"  Skipped: {len(result.skipped)}")

    return 0 if result.wasSuccessful() else 1


def main():
    os.chdir(ROOT_DIR)

    extra = sys.argv[1:]

    print("=" * 70)
    print("  COI Pipeline — Edge Case Test Suite")
    print("  Carolina Compliance Solutions")
    print("=" * 70)

    # Try pytest first, fall back to unittest
    try:
        import pytest  # noqa: F401
        has_pytest = True
    except ImportError:
        has_pytest = False

    if has_pytest:
        returncode = run_with_pytest(extra)
    else:
        returncode = run_with_unittest()

    print()
    build_html_report(REPORT_XML, REPORT_HTML)

    print()
    if returncode == 0:
        print("  All edge case tests passed.")
    else:
        print(f"  Some tests failed (exit code {returncode}).")
        print("  Review the HTML report for details on which edge cases")
        print("  the pipeline currently handles correctly vs. incorrectly.")

    return returncode


if __name__ == "__main__":
    sys.exit(main())
