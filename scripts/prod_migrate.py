#!/usr/bin/env python3

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import db


def main() -> int:
    print("开始执行生产数据库迁移...")
    db.init_tables()
    print("生产数据库迁移完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
