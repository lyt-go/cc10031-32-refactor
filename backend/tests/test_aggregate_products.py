"""归并产物识别共用约定的重构验证：

- 改动前后完整输入对比：更正版本按完整产物认定取代旧版，条数与形态稳定；
- 索引重建、迁冷接单、下载恢复在同一更正窗口内交叠进行；
- 途中各消费者（导出/索引/迁冷）实际采用的产物文件均可溯源。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services.aggregate_products import KIND_ROLLUP


@pytest.fixture()
def data_dir():
    """将数据目录指向临时位置，避免污染 real data。"""
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            yield Path(tmp)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(ln) for ln in f if ln.strip()]


# 两个测点；旧版第一窗为 20，更正版为 21，第二窗均为 30
POINTS = ["SENS-O1", "SENS-O2"]
OLD_WINDOW1 = 20
NEW_WINDOW1 = 21
WINDOW2 = 30


def _rollup_rows(point: str, window1_value: int) -> list[dict]:
    return [
        {"window_id": f"ROLL-{point}", "point": point, "window_type": "固定窗口",
         "window_start": 1, "window_end": 5, "record_count": 5,
         "rollup_value": window1_value, "metric": "均值"},
        {"window_id": f"ROLL-{point}", "point": point, "window_type": "固定窗口",
         "window_start": 2, "window_end": 10, "record_count": 5,
         "rollup_value": WINDOW2, "metric": "均值"},
    ]


def _seed_products(data_dir: Path) -> None:
    """准备两个点、各两个窗口结果（20/30），外加各一份降采样产物。"""
    for point in POINTS:
        _write_jsonl(data_dir / "rollup_windows" / f"ROLL-{point}_rollup.jsonl",
                     _rollup_rows(point, OLD_WINDOW1))
        _write_jsonl(
            data_dir / "downsampled" / f"DOWN-{point}_downsampled.jsonl",
            [{"task_id": f"DOWN-{point}", "point": point, "window_index": i + 1,
              "source_count": 10, "aggregated_value": 100 + i, "method": "均值",
              "target_freq": "1Hz"} for i in range(3)],
        )


def _snapshot(products) -> list[tuple]:
    """完整输入快照：(类别, 测点, 窗口, 数值, 来源文件)，用于改动前后对比。"""
    snap = []
    for p in products:
        if p.kind == KIND_ROLLUP:
            snap.append((p.kind, p.record.get("point"), p.record.get("window_start"),
                         p.record.get("rollup_value"), p.source_file))
        else:
            snap.append((p.kind, p.record.get("point"), p.record.get("window_index"),
                         p.record.get("aggregated_value"), p.source_file))
    return sorted(snap)


def test_corrected_version_supersedes_only_when_complete(data_dir):
    from app.services.aggregate_products import (
        plan_aggregate_products,
        read_product_files,
        scan_aggregate_products,
    )

    _seed_products(data_dir)
    rollup_dir = data_dir / "rollup_windows"
    ds_dir = data_dir / "downsampled"

    # 改动前的完整输入
    before = scan_aggregate_products(rollup_dir, ds_dir)
    before_snap = _snapshot(before)
    assert ("rollup", "SENS-O1", 1, 20, "ROLL-SENS-O1_rollup.jsonl") in before_snap
    assert len(before_snap) == 10  # 2点 × (2窗 + 3降采样)

    # 扫描期间加入新版归并文件：计划已生成、读取尚未开始
    plan = plan_aggregate_products(rollup_dir, ds_dir)
    _write_jsonl(rollup_dir / "ROLL-SENS-O1_rollup.v2.jsonl",
                 _rollup_rows("SENS-O1", NEW_WINDOW1))
    inflight = read_product_files(plan)  # 在途扫描仍按旧计划读取
    assert ("rollup", "SENS-O1", 1, 20, "ROLL-SENS-O1_rollup.jsonl") in _snapshot(inflight)
    assert all(pf.filename != "ROLL-SENS-O1_rollup.v2.jsonl" for pf in plan)

    # 改动后的完整输入：更正版整体取代旧版（来源文件切换），数值上仅第一窗 20 -> 21
    after = scan_aggregate_products(rollup_dir, ds_dir)
    after_snap = _snapshot(after)
    assert len(after_snap) == len(before_snap)  # 条数稳定：取代而非追加
    removed = [t for t in before_snap if t not in after_snap]
    added = [t for t in after_snap if t not in before_snap]
    assert removed == [("rollup", "SENS-O1", 1, 20, "ROLL-SENS-O1_rollup.jsonl"),
                       ("rollup", "SENS-O1", 2, 30, "ROLL-SENS-O1_rollup.jsonl")]
    assert added == [("rollup", "SENS-O1", 1, 21, "ROLL-SENS-O1_rollup.v2.jsonl"),
                     ("rollup", "SENS-O1", 2, 30, "ROLL-SENS-O1_rollup.v2.jsonl")]
    # 数值口径对比：仅第一窗变更，第二窗与其余测点完全未动
    before_values = {t[:3]: t[3] for t in before_snap}
    after_values = {t[:3]: t[3] for t in after_snap}
    assert before_values.keys() == after_values.keys()
    changed = [k for k in before_values if before_values[k] != after_values[k]]
    assert changed == [("rollup", "SENS-O1", 1)]

    # 不完整的更正版本（缺关键字段）没有取代资格，仍采用 v2
    _write_jsonl(rollup_dir / "ROLL-SENS-O1_rollup.v3.jsonl",
                 [{"window_id": "ROLL-SENS-O1", "point": "SENS-O1", "window_start": 1}])
    final_snap = _snapshot(scan_aggregate_products(rollup_dir, ds_dir))
    assert ("rollup", "SENS-O1", 1, 21, "ROLL-SENS-O1_rollup.v2.jsonl") in final_snap


def test_index_rebuild_migration_and_download_resume_overlap(data_dir):
    from app.services.series_export import SeriesExportService
    from app.services.series_index import SeriesIndexService
    from app.services.tier_storage import TierStorageService
    from app.store import store

    _seed_products(data_dir)

    # 一份下载在读取旧内容：导出启动并领取当前产物
    store.rows("series_export").append({
        "id": 301, "导出编号": "EXPORT-OVER", "测点范围": "SENS-O1,SENS-O2",
        "时间范围": "2026-09-01", "导出格式": "jsonl", "采样粒度": "1h",
        "记录条数": "0", "分片大小": "0", "导出水位": "0",
        "status": "待导出", "pending": True, "abnormal": False})
    export_svc = SeriesExportService()
    first = export_svc.export_series(301)
    assert first["records_exported"] == 10  # 2点 × (2窗 + 3降采样)
    assert first["sources"].count("ROLL-SENS-O1_rollup.jsonl") == 1
    delivered = _read_jsonl(data_dir / "exports" / "EXPORT-OVER_export.jsonl")
    o1w1 = [r for r in delivered if r["source"] == "rollup_window"
            and r["point"] == "SENS-O1" and r["window_index"] == 1]
    assert [r["value"] for r in o1w1] == [20]

    # 索引登记（归档侧将读取这份登记），冷迁移还未开始
    store.rows("series_index").append({
        "id": 302, "索引编号": "IDX-OVER", "测点编号": "SENS-O1",
        "时间分区": "2026-09-01", "点区间": "0-0", "索引粒度": "1h",
        "索引条数": "0", "存储位置": "", "索引版本": "v1.0",
        "status": "待构建", "pending": True, "abnormal": False})
    index_svc = SeriesIndexService()
    built = index_svc.build_index(302)
    assert built["indexed_count"] == 10
    index_rows = _read_jsonl(data_dir / "series_index" / "IDX-OVER_index.jsonl")
    assert [r["rollup_value"] for r in index_rows if r.get("point") == "SENS-O1"
            and r.get("window_index") == 1 and "rollup_value" in r] == [20]

    # 索引已登记、冷迁移未开始时：归并文件收到完整的更正版本（第一窗 20 -> 21）
    _write_jsonl(data_dir / "rollup_windows" / "ROLL-SENS-O1_rollup.v2.jsonl",
                 _rollup_rows("SENS-O1", NEW_WINDOW1))

    # 迁冷接单（索引重建发布前）：第一窗记待办原因，未被更正的第二窗独立迁冷
    store.rows("tier_storage").append({
        "id": 303, "策略编号": "TIER-OVER", "时间边界": "30d",
        "热区范围": "0-7d", "温区范围": "7d-30d", "冷区路径": "/cold/archive",
        "迁移方式": "自动迁移", "驻留天数": "30", "策略版本": "v1.0",
        "status": "待生效", "pending": True, "abnormal": False})
    tier_svc = TierStorageService()
    order1 = tier_svc.accept_cold_migration(303)
    receipts1 = {(r["point"], r["window_index"], r["kind"]): r
                 for r in order1["receipts"]}
    w1 = receipts1[("SENS-O1", 1, "rollup")]
    assert w1["decision"] == "待办"
    assert "更正" in w1["backlog_reason"]
    assert w1["pending_version"] == 2 and w1["pending_value"] == 21
    w2 = receipts1[("SENS-O1", 2, "rollup")]
    assert w2["decision"] == "已迁冷"
    assert w2["version_written"] == 1 and w2["value_written"] == 30
    assert order1["deferred_count"] == 1
    # 冷区归档里没有第一窗，第二窗已在
    cold_rows = _read_jsonl(data_dir / "tier_storage" / "TIER-OVER_cold_archive.jsonl")
    assert not [r for r in cold_rows if r.get("point") == "SENS-O1"
                and r.get("window_index") == 1 and "rollup_value" in r]
    assert [r for r in cold_rows if r.get("point") == "SENS-O1"
            and r.get("window_index") == 2 and "rollup_value" in r]

    # 下载恢复：仍按原领取读回 20，来源是旧版文件，条数保持原样
    resumed = export_svc.resume_export(301)
    assert resumed["resumed"] is True
    assert resumed["records_exported"] == first["records_exported"]
    assert "ROLL-SENS-O1_rollup.jsonl" in resumed["sources"]
    assert "ROLL-SENS-O1_rollup.v2.jsonl" not in resumed["sources"]
    redelivered = _read_jsonl(data_dir / "exports" / "EXPORT-OVER_export.jsonl")
    o1w1_resumed = [r for r in redelivered if r["source"] == "rollup_window"
                    and r["point"] == "SENS-O1" and r["window_index"] == 1]
    assert [r["value"] for r in o1w1_resumed] == [20]

    # 索引重建：发布更正版本（整文件重写，不叠加旧窗），来源切换到 v2
    rebuilt = index_svc.build_index(302)
    assert rebuilt["indexed_count"] == 10
    assert "ROLL-SENS-O1_rollup.v2.jsonl" in rebuilt["sources"]
    rebuilt_rows = _read_jsonl(data_dir / "series_index" / "IDX-OVER_index.jsonl")
    o1w1_rows = [r for r in rebuilt_rows if r.get("point") == "SENS-O1"
                 and r.get("window_index") == 1 and "rollup_value" in r]
    assert [r["rollup_value"] for r in o1w1_rows] == [21]
    assert len(rebuilt_rows) == 10

    # 重建发布后再次接单：第一窗按更正版迁冷，记录实际写入版本 v2
    order2 = tier_svc.accept_cold_migration(303)
    receipts2 = {(r["point"], r["window_index"], r["kind"]): r
                 for r in order2["receipts"]}
    w1_after = receipts2[("SENS-O1", 1, "rollup")]
    assert w1_after["decision"] == "已迁冷"
    assert w1_after["version_written"] == 2 and w1_after["value_written"] == 21
    assert order2["deferred_count"] == 0
    # 已获准迁冷的范围可复核：两次接单的收据均留痕
    receipt_state = json.loads(
        (data_dir / "checkpoints" / "tier_storage_TIER-OVER_receipts.json")
        .read_text(encoding="utf-8"))
    assert len(receipt_state["runs"]) == 2
    assert receipt_state["runs"][0]["deferred"] == 1
    assert receipt_state["runs"][1]["deferred"] == 0
