"""序列索引领域服务：维护时序数据按时间分区的点区间索引与存储位置登记。
按共用产物约定扫描归并/降采样结果构建序列索引分区并写入索引元数据文件。"""
from __future__ import annotations

import time
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


class SeriesIndexService(BaseModuleService):
    MODULE = "series_index"
    ENTITY = "索引分区"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["索引编号", "测点编号", "时间分区", "点区间", "索引粒度", "索引条数", "存储位置", "索引版本"]
    REQUIRED_FIELDS = ["索引编号", "测点编号", "时间分区"]
    STATUS_ORDER = ["草稿", "待构建", "已构建", "已失效"]
    ACTION_RULES = {"启动构建": "待构建", "确认构建": "已构建", "标记失效": "已失效"}
    # 数据存储操作：扫描归并窗口结果按时间分区构建序列索引分区写入索引元数据
    NEGATIVE_ACTIONS = ["标记失效"]

    def _scan_products(self) -> tuple[list[ProductFile], list[ProductRecord]]:
        """按共用约定扫描归并与降采样产物（各自取当前完整版本）。"""
        rollup_dir = self._data_dir("rollup_windows")
        ds_dir = self._data_dir("downsampled")
        plan = plan_aggregate_products(rollup_dir, ds_dir)
        return plan, read_product_files(plan)

    @staticmethod
    def _to_index_rows(products: list[ProductRecord], entry: dict[str, Any],
                       partition: str) -> list[dict[str, Any]]:
        """索引侧自己的登记形态（投入归档的资格），字段口径保持不变。"""
        point_key = entry["测点编号"]
        rows: list[dict[str, Any]] = []
        for p in products:
            r = p.record
            if p.kind == KIND_ROLLUP:
                rows.append({
                    "index_id": entry["索引编号"],
                    "point": r.get("point", point_key),
                    "time_partition": partition,
                    "window_index": r.get("window_start", 0),
                    "record_count": r.get("record_count", 0),
                    "rollup_value": r.get("rollup_value", 0),
                })
            else:
                rows.append({
                    "index_id": entry["索引编号"],
                    "point": r.get("point", point_key),
                    "time_partition": partition,
                    "window_index": r.get("window_index", 0),
                    "aggregated_value": r.get("aggregated_value", 0),
                })
        return rows

    def build_index(self, index_id: int) -> dict[str, Any]:
        """按时间分区扫描归并/降采样产物构建序列索引分区；重建时整文件发布。"""
        entry = store.find(self.MODULE, index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        self.run_action(index_id, "启动构建")
        point_key = entry["测点编号"]
        partition = entry.get("时间分区", time.strftime("%Y-%m-%d", time.localtime()))

        # 共用约定扫描归并窗口与降采样数据文件
        plan, products = self._scan_products()
        # 数据产物输出：将索引扫描结果写入序列索引数据JSONL文件
        indexed_points = self._to_index_rows(products, entry, partition)

        # 索引重建发布：整文件重写，归档侧读到的始终是最近一次登记口径
        output_file = self._data_dir("series_index") / f"{entry['索引编号']}_index.jsonl"
        written = self._rewrite_jsonl(output_file, indexed_points)
        # 登记来源留痕：归档复核与迁冷接单据此认定每窗实际登记的产物版本
        self._write_checkpoint(
            f"series_index_{entry['索引编号']}_sources",
            {
                "index_id": entry["索引编号"],
                "sources": [
                    {"product": pf.product, "kind": pf.kind,
                     "version": pf.version, "file": pf.filename}
                    for pf in plan
                ],
                "windows": {
                    f"{p.kind}|{p.record.get('point', point_key)}|"
                    f"{p.record.get('window_start' if p.kind == KIND_ROLLUP else 'window_index', 0)}": {
                        "kind": p.kind,
                        "version": p.version,
                        "file": p.source_file,
                    }
                    for p in products
                },
            },
        )
        entry["索引条数"] = str(written)
        entry["点区间"] = f"1-{written}"
        entry["存储位置"] = str(output_file)
        entry["索引版本"] = "v1.0"
        self.run_action(index_id, "确认构建")
        return {"index_id": entry["索引编号"], "point": point_key,
                "partition": partition, "indexed_count": written,
                "sources": [pf.filename for pf in plan]}

    def build_all_partitions(self, point_key: str) -> dict[str, Any]:
        """为指定测点构建所有时间分区的序列索引数据集。"""
        partitions = ["2026-09-01", "2026-09-02", "2026-09-03"]
        results = []
        for part in partitions:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "索引编号": f"IDX-{point_key}-{part}",
                "测点编号": point_key,
                "时间分区": part,
                "点区间": "0-0",
                "索引粒度": "1h",
                "索引条数": "0",
                "存储位置": "",
                "索引版本": "v1.0",
            })
            r = self.build_index(tid)
            results.append(r)
        return {"point": point_key, "partitions_built": len(results), "results": results}
