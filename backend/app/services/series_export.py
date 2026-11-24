"""序列导出领域服务：维护时序结果按测点与时间范围的分片导出任务与导出水位。
按测点范围和时间窗口选择性导出归并数据，写入分片导出数据文件。"""
from __future__ import annotations

from typing import Any

from app.services.aggregate_products import (
    KIND_ROLLUP,
    ProductFile,
    ProductRecord,
    plan_aggregate_products,
    read_product_files,
)
from app.services.base import BaseModuleService
from app.store import store


class SeriesExportService(BaseModuleService):
    MODULE = "series_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["导出编号", "测点范围", "时间范围", "导出格式", "采样粒度", "记录条数", "分片大小", "导出水位"]
    REQUIRED_FIELDS = ["导出编号", "测点范围", "时间范围"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    # 数据导出操作：按测点范围时间窗口导出降采样与归并结果到分片导出文件
    ACTION_RULES = {"启动导出": "导出中", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    def _scan_products(self) -> tuple[list[ProductFile], list[ProductRecord]]:
        """按共用约定扫描归并与降采样产物（各自取当前完整版本）。"""
        rollup_dir = self._data_dir("rollup_windows")
        ds_dir = self._data_dir("downsampled")
        plan = plan_aggregate_products(rollup_dir, ds_dir)
        return plan, read_product_files(plan)

    @staticmethod
    def _to_export_rows(products: list[ProductRecord], export_key: str) -> list[dict[str, Any]]:
        """导出侧自己的交付形态（对外交付值班员的资格），字段口径保持不变。"""
        rows: list[dict[str, Any]] = []
        for p in products:
            r = p.record
            if p.kind == KIND_ROLLUP:
                rows.append({
                    "export_id": export_key,
                    "source": "rollup_window",
                    "point": r.get("point", ""),
                    "window_index": r.get("window_start", 0),
                    "value": r.get("rollup_value", 0),
                    "metric": r.get("metric", ""),
                })
            else:
                rows.append({
                    "export_id": export_key,
                    "source": "downsample",
                    "point": r.get("point", ""),
                    "window_index": r.get("window_index", 0),
                    "value": r.get("aggregated_value", 0),
                    "method": r.get("method", ""),
                })
        return rows

    def export_series(self, export_id: int) -> dict[str, Any]:
        """按测点范围和时间窗口导出降采样与归并结果数据到分片导出交付文件。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        self.run_action(export_id, "启动导出")
        export_fmt = task.get("导出格式", "jsonl")

        # 共用约定扫描归并窗口和降采样数据文件导出
        plan, products = self._scan_products()
        # 数据产物输出：写入导出结果为分片交付JSONL数据文件
        exported = self._to_export_rows(products, task["导出编号"])
        # 登记领取：本次导出实际采用的产物文件，下载恢复按原领取读回
        self._write_checkpoint(
            f"series_export_{task['导出编号']}_claim",
            {
                "export_id": task["导出编号"],
                "claimed_files": [
                    {"product": pf.product, "kind": pf.kind,
                     "version": pf.version, "file": pf.filename}
                    for pf in plan
                ],
            },
        )

        # 将导出结果写入分片导出交付文件
        output_file = self._data_dir("exports") / f"{task['导出编号']}_export.{export_fmt}"
        written = self._write_jsonl(output_file, exported)
        task["记录条数"] = str(written)
        task["分片大小"] = str(len(exported) * 80)
        task["导出水位"] = str(written)
        self.run_action(export_id, "确认交付")
        return {"export_id": task["导出编号"], "format": export_fmt,
                "records_exported": written, "total_size_estimate": len(exported) * 80,
                "sources": [pf.filename for pf in plan]}

    def resume_export(self, export_id: int) -> dict[str, Any]:
        """下载恢复：按原领取的产物文件读回交付内容，即使归并产物已有更正版本。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        claim = self._read_checkpoint(f"series_export_{task['导出编号']}_claim")
        if not claim:
            return {"error": f"导出任务 {task['导出编号']} 没有领取记录，无法按原领取恢复"}
        export_fmt = task.get("导出格式", "jsonl")

        # 只读回原领取的文件；文件名即版本约定，新到的更正版本不影响恢复
        claimed_paths = []
        for item in claim.get("claimed_files", []):
            sub = "rollup_windows" if item.get("kind") == KIND_ROLLUP else "downsampled"
            claimed_paths.append(self._data_dir(sub) / item["file"])
        products = read_product_files(claimed_paths)
        exported = self._to_export_rows(products, task["导出编号"])

        # 恢复交付：整文件重写，保证同一领取重复恢复不产生重复行
        output_file = self._data_dir("exports") / f"{task['导出编号']}_export.{export_fmt}"
        written = self._rewrite_jsonl(output_file, exported)
        task["记录条数"] = str(written)
        task["分片大小"] = str(len(exported) * 80)
        task["导出水位"] = str(written)
        return {"export_id": task["导出编号"], "format": export_fmt,
                "records_exported": written, "total_size_estimate": len(exported) * 80,
                "resumed": True, "sources": [p.name for p in claimed_paths]}

    def export_by_time_window(self, point_range: str, time_window: str,
                               fmt: str = "jsonl") -> dict[str, Any]:
        """按指定时间窗口和测点范围创建导出任务并输出交付数据。"""
        tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
        self._auto_create_seed(self.MODULE, {
            "导出编号": f"EXPORT-{point_range}-{tid}",
            "测点范围": point_range,
            "时间范围": time_window,
            "导出格式": fmt,
            "采样粒度": "1h",
            "记录条数": "0",
            "分片大小": "0",
            "导出水位": "0",
        })
        return self.export_series(tid)
