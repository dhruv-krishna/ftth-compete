"""Run all refresh pipelines in sequence (BDC, IAS, ACS, TIGER, Ookla).

Idempotent: skips downloads when local copies match the latest published release.
"""

from __future__ import annotations


def main() -> None:
    print("TODO: implement refresh_all")


if __name__ == "__main__":
    main()
