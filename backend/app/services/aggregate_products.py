"""归并与降采样数据产物的共用识别约定。

序列索引（登记后投入归档）与序列导出（对外交付值班员）原先各自写了一遍
产物目录遍历；这里把"哪些文件算归并/降采样产物、哪一版是完整产物"收拢成
一份共用约定。两侧各自保留投入归档或外部交付的资格与产出形态，产物认定
只此一份。

约定要点：
- 产物文件命名：``{产物编号}_rollup[.vN].jsonl`` 与
  ``{产物编号}_downsampled[.vN].jsonl``；不带版本后缀的视为第 1 版。
- 完整产物认定：文件每行可解析且每条记录具备该类关键字段，该文件才算
  一个完整产物版本；更正版本只有完整才有资格取代旧版。
- 同一产物取最高的完整版本作为当前产物；若所有版本都不完整，退回最高
  版本按原样读取（保持既有数据集条数不变）。
- 扫描分计划（plan）与读取（read）两步：扫描期间到达的新版本不影响
  已在进行的扫描，下一轮扫描才会采用。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

KIND_ROLLUP = "rollup"
KIND_DOWNSAMPLED = "downsampled"

_PRODUCT_RE = re.compile(
    r"^(?P<product>.+?)_(?P<kind>rollup|downsampled)(?:\.v(?P<version>[1-9]\d*))?\.jsonl$"
)

# 完整产物认定：各类产物记录必须具备的关键字段
REQUIRED_FIELDS = {
    KIND_ROLLUP: ("window_start", "rollup_value"),
    KIND_DOWNSAMPLED: ("window_index", "aggregated_value"),
}


@dataclass(frozen=True)
class ProductFile:
    """一个产物版本文件。"""

    product: str
    kind: str
    version: int
    path: Path

    @property
    def filename(self) -> str:
        return self.path.name


@dataclass(frozen=True)
class ProductRecord:
    """一条带来源信息的产物记录：消费者可据此说明实际采用了哪些文件。"""

    product: str
    kind: str
    version: int
    source_file: str
    record: dict[str, Any]


def parse_product_filename(path: Path) -> ProductFile | None:
    """按命名约定解析产物文件名；不符合约定返回 None。"""
    m = _PRODUCT_RE.match(Path(path).name)
    if not m:
        return None
    return ProductFile(
        product=m.group("product"),
        kind=m.group("kind"),
        version=int(m.group("version") or 1),
        path=Path(path),
    )


def is_complete_record(kind: str, record: dict[str, Any]) -> bool:
    """单条记录的完整性认定：具备该类产物的关键字段。"""
    return all(field in record for field in REQUIRED_FIELDS[kind])


def is_complete_product_file(path: Path, kind: str) -> bool:
    """完整产物认定：文件每行可解析且每条记录具备关键字段。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except OSError:
        return False
    if not lines:
        return False
    for ln in lines:
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            return False
        if not isinstance(rec, dict) or not is_complete_record(kind, rec):
            return False
    return True


def list_product_files(rollup_dir: Path, downsample_dir: Path) -> list[ProductFile]:
    """列出两个产物目录下的全部候选产物版本文件（含被取代的旧版）。"""
    files: list[ProductFile] = []
    for directory in (rollup_dir, downsample_dir):
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.jsonl")):
            pf = parse_product_filename(path)
            if pf is not None:
                files.append(pf)
    return files


def plan_aggregate_products(rollup_dir: Path, downsample_dir: Path) -> list[ProductFile]:
    """扫描计划：同一产物取最高的完整版本；都不完整时退回最高版本。"""
    by_product: dict[tuple[str, str], list[ProductFile]] = {}
    for pf in list_product_files(rollup_dir, downsample_dir):
        by_product.setdefault((pf.product, pf.kind), []).append(pf)
    plan: list[ProductFile] = []
    for (product, kind), versions in by_product.items():
        versions.sort(key=lambda pf: pf.version)
        complete = [pf for pf in versions if is_complete_product_file(pf.path, kind)]
        plan.append(complete[-1] if complete else versions[-1])
    # 归并产物在前、降采样产物在后，各自按文件名排序，保证消费顺序稳定
    plan.sort(key=lambda pf: (0 if pf.kind == KIND_ROLLUP else 1, pf.filename))
    return plan


def read_product_files(files: Iterable[ProductFile | Path]) -> list[ProductRecord]:
    """按计划读取产物记录并附来源；接受 ProductFile 或路径（按文件名解析版本）。"""
    records: list[ProductRecord] = []
    for item in files:
        pf = item if isinstance(item, ProductFile) else parse_product_filename(Path(item))
        if pf is None or not pf.path.exists():
            continue
        with open(pf.path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    rec = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    records.append(
                        ProductRecord(pf.product, pf.kind, pf.version, pf.filename, rec)
                    )
    return records


def scan_aggregate_products(rollup_dir: Path, downsample_dir: Path) -> list[ProductRecord]:
    """一步扫描：当前计划 + 读取，供索引登记与导出交付使用。"""
    return read_product_files(plan_aggregate_products(rollup_dir, downsample_dir))
