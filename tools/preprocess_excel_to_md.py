# tools/preprocess_excel_to_md.py
# 依存: pandas, openpyxl, pyyaml (任意)
import argparse
import os
from pathlib import Path
import hashlib
import pandas as pd
import yaml
from datetime import datetime

# === 設定 ===
SUPPORTED_EXT = {".xlsx", ".xlsm", ".xls"}  # 必要ならxls→xlrdが要るが基本xlsx想定
MAX_CHARS_PER_CHUNK = 8000  # 1ファイルが大きい場合、KB検索のために分割

def infer_doc_type(path: Path) -> str:
    name = path.stem.lower()
    if "観点" in path.name or "view" in name or "test" in name:
        return "test_viewpoints"
    if "設計" in path.name or "design" in name or "仕様" in path.name:
        return "design_spec"
    return "unknown"

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    # 罫線用のNaN→空、日付→ISO、前方埋めで結合セルぽさを補正
    df = df.copy()
    df = df.where(pd.notnull(df), "")
    # 日付型の見やすい変換
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    # 先頭数列は前方埋めして階層見出しに対応（必要に応じて列数調整）
    if len(df.columns) > 0:
        df[df.columns[:2]] = df[df.columns[:2]].ffill()
    return df

def df_to_markdown(df: pd.DataFrame) -> str:
    # pandas 2.x の to_markdown は tabulate 同梱、インデックス消す
    return df.to_markdown(index=False)

def make_front_matter(meta: dict) -> str:
    # YAMLフロントマター
    return "---\n" + yaml.safe_dump(meta, allow_unicode=True, sort_keys=False) + "---\n\n"

def chunk_text(text: str, max_chars: int):
    if len(text) <= max_chars:
        return [text]
    # 見出し（##）や行単位で気持ちよく切る
    lines = text.splitlines(keepends=True)
    chunks, buf, size = [], [], 0
    for line in lines:
        # チャンク境界の候補
        hard_boundary = line.strip().startswith(("## ", "### ", "|"))
        if size > 0 and (size + len(line) > max_chars) and hard_boundary:
            chunks.append("".join(buf))
            buf, size = [], 0
        buf.append(line)
        size += len(line)
        if size >= max_chars:
            chunks.append("".join(buf))
            buf, size = [], 0
    if buf:
        chunks.append("".join(buf))
    return chunks

def process_excel(xlsx_path: Path, out_dir: Path):
    doc_type = infer_doc_type(xlsx_path)
    xls = pd.ExcelFile(xlsx_path)
    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        df = sanitize_df(df)

        # メタデータ
        meta = {
            "source_file": xlsx_path.name,
            "sheet": str(sheet),
            "doc_type": doc_type,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }

        # シート先頭に大見出し
        body = [f"# {xlsx_path.stem} - {sheet}\n\n"]
        # テーブルをMarkdown化（表が空でなければ）
        if not df.empty:
            body.append(df_to_markdown(df))
            body.append("\n")
        md_core = "".join(body)

        # フロントマター付き本文
        fm = make_front_matter(meta)
        full_md = fm + md_core

        # 大きい場合は分割
        chunks = chunk_text(full_md, MAX_CHARS_PER_CHUNK)
        for i, chunk in enumerate(chunks, start=1):
            base = f"{xlsx_path.stem}__{sheet}"
            if len(chunks) > 1:
                base += f"__part{i:02d}"
            # 衝突回避のためハッシュも付けられる（任意）
            h = hashlib.md5(chunk.encode("utf-8")).hexdigest()[:8]
            out_name = f"{base}__{h}.md"
            (out_dir / out_name).write_text(chunk, encoding="utf-8")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", required=True, help="Excelを含む入力ディレクトリ（例: docs）")
    parser.add_argument("--out-dir", required=True, help="Markdown出力ディレクトリ（例: out_md）")
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for p in in_dir.rglob("*"):
        if p.suffix.lower() in SUPPORTED_EXT:
            process_excel(p, out_dir)
            count += 1

    print(f"Processed {count} Excel file(s) → {out_dir}")

if __name__ == "__main__":
    main()

