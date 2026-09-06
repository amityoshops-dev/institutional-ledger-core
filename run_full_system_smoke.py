import subprocess
import sys

scripts = [
    "send_test_webhook.py",
    "run_all_remaining_tests.py",
    "test_tolerance_split.py",
    "test_immutability.py",
    "test_reversal.py",
    "test_as_of_audit.py",
    "test_outward_edge_cases.py",
    "audit_ledger.py"
]

print("=" * 80)
print("STARTING FULL SYSTEM REGRESSION & INVARIANT AUDIT SUITE")
print("=" * 80)

failed = []
python_exe = sys.executable

for s in scripts:
    print(f"\n[RUNNING] {s} ...")
    res = subprocess.run([python_exe, s], capture_output=False)
    if res.returncode != 0:
        failed.append(s)

print("\n" + "=" * 80)
if not failed:
    print("ALL VERIFICATION CHECKS PASSED: SYSTEM FULLY OPERATIONAL & INVARIANTS INTACT")
else:
    print(f"FAILED CHECKS: {', '.join(failed)}")
print("=" * 80)
