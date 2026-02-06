#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤1：重新开始 + 整理列（并行加速版）

使用ThreadPoolExecutor实现并行化，适合I/O密集型的CSV文件读写操作
"""

import pandas as pd
from pathlib import Path
import shutil
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from datetime import timedelta

# 线程安全的日志，统一格式：小节用 ---，子项缩进 2 空格
log_lock = threading.Lock()
SEP = "=" * 80
SUB = "-" * 60

def log(msg):
    with log_lock:
        print(msg)
        sys.stdout.flush()

BASE_DIR = Path(__file__).resolve().parent  # 项目根目录
SOURCE_DIR = BASE_DIR / "原始数据"
HEALTH_OUTPUT = BASE_DIR / "Health"
HYPO_OUTPUT = BASE_DIR / "HYPO"

# 定义原始文件夹
health_folders = [
    SOURCE_DIR / "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-04-06 140118_10",
    SOURCE_DIR / "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖2_2024-02-06 183001_21",
    SOURCE_DIR / "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-02-05 155459_39"
]

hypo_folders = [
    SOURCE_DIR / "指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-04-06 140331_83",
    SOURCE_DIR / "指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-02-06 183111_28"
]

# 删除冗余列的列表
COLUMNS_TO_DROP = [
    'patient_sn', '分组名称', 'time_quantum', '时间段', 'group_name',
    'test_gender_time_quantum1', 'test_出生日期_time_quantum1', 'test_death_time_quantum1',
    'test_性别_时间段1', 'test_出生日期_时间段1', 'test_是否死亡_时间段1',
    '低血糖_gender_time_quantum1', '低血糖_出生日期_time_quantum1', '低血糖_death_time_quantum1',
    '低血糖_性别_时间段1', '低血糖_出生日期_时间段1', '低血糖_是否死亡_时间段1',
]

# 文件重命名映射
rename_map = {
    '基线数据.test_全部诊断.csv': '诊断.csv',
    '基线数据.test_药品类医嘱.csv': '药品医嘱.csv',
    '基线数据.test_非药品类医嘱.csv': '非药品医嘱.csv',
    '基线数据.test_其他检验.csv': '其他检验.csv',
    '基线数据.test_生命体征.csv': '生命体征.csv',
    '基线数据.test_血糖单.csv': '血糖.csv',
    '基线数据.test_入院记录.csv': '入院记录.csv',
    '基线数据.test_糖代谢测定.csv': '糖代谢.csv',
    '基线数据.test_住院.csv': '住院.csv',
    '基线数据.test_C反应蛋白检测.csv': 'CRP.csv',
    '基线数据.test_血常规.csv': '血常规.csv',
    '基线数据.test_生化检查.csv': '生化.csv',
    '基线数据.低血糖_全部诊断.csv': '诊断.csv',
    '基线数据.低血糖_药品类医嘱.csv': '药品医嘱.csv',
    '基线数据.低血糖_非药品类医嘱.csv': '非药品医嘱.csv',
    '基线数据.低血糖_其他检验.csv': '其他检验.csv',
    '基线数据.低血糖_生命体征.csv': '生命体征.csv',
    '基线数据.低血糖_血糖单.csv': '血糖.csv',
    '基线数据.低血糖_入院记录.csv': '入院记录.csv',
    '基线数据.低血糖_糖代谢测定.csv': '糖代谢.csv',
    '基线数据.低血糖_住院.csv': '住院.csv',
    '基线数据.低血糖_C反应蛋白检测.csv': 'CRP.csv',
    '基线数据.低血糖_血常规.csv': '血常规.csv',
    '基线数据.低血糖_生化检查.csv': '生化.csv',
}

def merge_files(folders, file_name, output_path):
    """合并多个文件夹中的同名文件"""
    try:
        dfs = []
        for folder in folders:
            file_path = folder / file_name
            if file_path.exists():
                df = pd.read_csv(file_path, skiprows=2, low_memory=False)
                dfs.append(df)
        
        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            merged.to_csv(output_path, index=False, encoding='utf-8-sig')
            return (file_name, len(merged), True)
        return (file_name, 0, False)
    except Exception as e:
        return (file_name, 0, False, str(e))

def merge_other_test_files(folders, patterns, output_path):
    """合并其他检验文件"""
    try:
        dfs = []
        for file_name in patterns:
            for folder in folders:
                file_path = folder / file_name
                if file_path.exists():
                    df = pd.read_csv(file_path, skiprows=2, low_memory=False)
                    dfs.append(df)
        
        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            merged.to_csv(output_path, index=False, encoding='utf-8-sig')
            return (output_path.name, len(merged), True)
        return (output_path.name, 0, False)
    except Exception as e:
        return (output_path.name, 0, False, str(e))

def process_admission_key(file_path):
    """添加admission_key"""
    try:
        df = pd.read_csv(file_path, low_memory=False)
        orig_cols = len(df.columns)
        
        if 'patient_sn' not in df.columns or 'time_quantum' not in df.columns:
            return (file_path.name, False, "缺少必需列")
        
        df['admission_key'] = df['patient_sn'].astype(str) + "_" + df['time_quantum'].astype(str)
        
        drop_cols = ['patient_sn', 'time_quantum', 'group_name']
        df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True, errors='ignore')
        
        cols = ['admission_key'] + [col for col in df.columns if col != 'admission_key']
        df = df[cols]
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        return (file_path.name, True, f"{orig_cols}→{len(df.columns)}列")
    except Exception as e:
        return (file_path.name, False, str(e))

def rename_file(old_path, new_name):
    """重命名文件"""
    try:
        new_path = old_path.parent / new_name
        old_path.rename(new_path)
        return (new_name, True)
    except Exception as e:
        return (new_name, False, str(e))

def clean_columns(file_path):
    """整理列（跳过药品医嘱文件）"""
    try:
        # 如果是药品医嘱文件，跳过列删除，只调整列顺序
        if '药品医嘱' in file_path.name or '药品' in file_path.name:
            df = pd.read_csv(file_path, low_memory=False)
            orig_cols = len(df.columns)
            
            # 只确保admission_key是第一列
            if 'admission_key' in df.columns and df.columns[0] != 'admission_key':
                cols = ['admission_key'] + [col for col in df.columns if col != 'admission_key']
                df = df[cols]
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            return (file_path.name, orig_cols, len(df.columns), 0)  # 删除0列
        
        # 其他文件正常处理
        df = pd.read_csv(file_path, low_memory=False)
        orig_cols = len(df.columns)
        
        cols_to_drop = [col for col in COLUMNS_TO_DROP if col in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
        
        empty_cols = df.columns[df.isna().all()].tolist()
        if empty_cols:
            df = df.drop(columns=empty_cols)
        
        if 'admission_key' in df.columns and df.columns[0] != 'admission_key':
            cols = ['admission_key'] + [col for col in df.columns if col != 'admission_key']
            df = df[cols]
        
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        return (file_path.name, orig_cols, len(df.columns), orig_cols - len(df.columns))
    except Exception as e:
        return (file_path.name, 0, 0, 0, str(e))


def _extract_campus(dept_str):
    """从科室名称中提取院区（括号中的内容），如 科室（外滩）→ 外滩。"""
    if pd.isna(dept_str):
        return None
    s = str(dept_str).strip().replace("\t", "")
    m = re.search(r"[（(]([^)）]+)[)）]", s)
    return m.group(1).strip() if m else None


def _process_datetime_columns(df):
    """日期8-8规则：识别并转换时间列，00:00-07:59 归入前一日，输出 YYYY-MM-DD。"""
    datetime_cols = []
    exclude_patterns = ['birth_date', 'admission_date', 'discharge_date', 'surgery_dates']
    for col in df.columns:
        col_lower = col.lower()
        if any(k in col_lower for k in ['time', '_date']) and \
           not any(p in col_lower for p in exclude_patterns):
            sample = df[col].dropna().astype(str).head(5)
            if len(sample) > 0:
                has_time = sample.str.contains(':', na=False).any() or (sample.str.len() > 10).any()
                if has_time:
                    datetime_cols.append(col)
    if not datetime_cols:
        return df, {}
    stats = {}
    for col in datetime_cols:
        df[col] = df[col].astype(str).str.replace('\t', '', regex=False).str.strip()
        df[col] = pd.to_datetime(df[col], errors='coerce')
        mask_early = df[col].dt.hour < 8
        df.loc[mask_early, col] = df.loc[mask_early, col] - timedelta(days=1)
        df[col] = df[col].dt.strftime('%Y-%m-%d')
        stats[col] = df[col].notna().sum()
    return df, stats


if __name__ == '__main__':
    log(SEP)
    log("步骤1：重新开始 + 整理列 + 日期8-8 转换")
    log(SEP)
    start_time = time.time()

    # 步骤1: 清理旧数据
    log("\n" + SUB)
    log("步骤1a: 清理旧数据")
    log(SUB)
    if HEALTH_OUTPUT.exists():
        shutil.rmtree(HEALTH_OUTPUT)
        log("  ✓ 删除 Health 目录")
    if HYPO_OUTPUT.exists():
        shutil.rmtree(HYPO_OUTPUT)
        log("  ✓ 删除 HYPO 目录")
    HEALTH_OUTPUT.mkdir(exist_ok=True)
    HYPO_OUTPUT.mkdir(exist_ok=True)
    log("  ✓ 创建输出目录")

    # 步骤2: 并行合并文件
    log("\n" + SUB)
    log("步骤1b: 并行合并文件")
    log(SUB)
    max_workers = 8
    log(f"  线程数: {max_workers}")
    merge_start = time.time()
    
    # Health组普通文件（排除基线数据.test.csv，因为信息包含在入院记录中且缺失住院时间段）
    health_files = [
        '基线数据.test_全部诊断.csv',
        '基线数据.test_药品类医嘱.csv',
        '基线数据.test_非药品类医嘱.csv',
        '基线数据.test_生命体征.csv',
        '基线数据.test_血糖单.csv',
        '基线数据.test_入院记录.csv',
        '基线数据.test_糖代谢测定.csv',
        '基线数据.test_住院.csv',
        '基线数据.test_C反应蛋白检测.csv',
        '基线数据.test_血常规.csv',
        '基线数据.test_生化检查.csv',
    ]
    
    # HYPO组普通文件（排除基线数据.低血糖.csv，因为信息包含在入院记录中且缺失住院时间段）
    hypo_files = [
        '基线数据.低血糖_全部诊断.csv',
        '基线数据.低血糖_药品类医嘱.csv',
        '基线数据.低血糖_非药品类医嘱.csv',
        '基线数据.低血糖_生命体征.csv',
        '基线数据.低血糖_血糖单.csv',
        '基线数据.低血糖_入院记录.csv',
        '基线数据.低血糖_糖代谢测定.csv',
        '基线数据.低血糖_住院.csv',
        '基线数据.低血糖_C反应蛋白检测.csv',
        '基线数据.低血糖_血常规.csv',
        '基线数据.低血糖_生化检查.csv',
    ]
    
    health_other_patterns = [
        '基线数据.test_其他检验.csv',
        '基线数据.test_其他检验_(1).csv',
        '基线数据.test_其他检验_(2).csv',
        '基线数据.test_其他检验_(3).csv',
    ]
    
    hypo_other_patterns = [
        '基线数据.低血糖_其他检验.csv',
        '基线数据.低血糖_其他检验_(1).csv',
        '基线数据.低血糖_其他检验_(2).csv',
        '基线数据.低血糖_其他检验_(3).csv',
    ]
    
    tasks = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Health组任务
        for fname in health_files:
            tasks.append(executor.submit(merge_files, health_folders, fname, HEALTH_OUTPUT / fname))
        
        # HYPO组任务
        for fname in hypo_files:
            tasks.append(executor.submit(merge_files, hypo_folders, fname, HYPO_OUTPUT / fname))
        
        # 其他检验文件
        tasks.append(executor.submit(merge_other_test_files, health_folders, health_other_patterns, 
                                     HEALTH_OUTPUT / '基线数据.test_其他检验.csv'))
        tasks.append(executor.submit(merge_other_test_files, hypo_folders, hypo_other_patterns, 
                                     HYPO_OUTPUT / '基线数据.低血糖_其他检验.csv'))
        
        health_count = 0
        hypo_count = 0
        
        for future in as_completed(tasks):
            result = future.result()
            if len(result) >= 3 and result[2]:
                fname, rows, _ = result[:3]
                group = "Health" if 'test' in fname or fname == '其他检验.csv' else "HYPO"
                if group == "Health":
                    health_count += 1
                else:
                    hypo_count += 1
                log(f"  ✓ [{group}] {fname}: {rows:,} 行")
    merge_time = time.time() - merge_start
    log(f"  ✓ Health: {health_count} 个文件, HYPO: {hypo_count} 个文件, 耗时: {merge_time:.1f}s")

    # 步骤3: 并行构建 admission_key
    log("\n" + SUB)
    log("步骤1c: 构建 admission_key")
    log(SUB)
    
    admission_start = time.time()
    all_files = list(HEALTH_OUTPUT.glob("*.csv")) + list(HYPO_OUTPUT.glob("*.csv"))
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_admission_key, f) for f in all_files]
        
        success = 0
        for future in as_completed(futures):
            fname, ok, msg = future.result()
            if ok:
                success += 1
                log(f"  ✓ {fname}: {msg}")
    admission_time = time.time() - admission_start
    log(f"  ✓ 完成: {success}/{len(all_files)} 个文件, 耗时: {admission_time:.1f}s")

    # 步骤4: 并行重命名文件
    log("\n" + SUB)
    log("步骤1d: 重命名文件")
    log(SUB)
    
    rename_start = time.time()
    rename_tasks = []
    
    for old_name, new_name in rename_map.items():
        for directory in [HEALTH_OUTPUT, HYPO_OUTPUT]:
            old_path = directory / old_name
            if old_path.exists():
                rename_tasks.append((old_path, new_name))
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(rename_file, old, new) for old, new in rename_tasks]
        
        rename_count = 0
        for future in as_completed(futures):
            result = future.result()
            if len(result) == 2 and result[1]:
                rename_count += 1
                log(f"  ✓ {result[0]}")
    rename_time = time.time() - rename_start
    log(f"  ✓ 重命名: {rename_count} 个文件, 耗时: {rename_time:.1f}s")

    # 步骤5: 并行整理列
    log("\n" + SUB)
    log("步骤1e: 整理列（删冗余列）")
    log(SUB)
    
    clean_start = time.time()
    all_files = list(HEALTH_OUTPUT.glob("*.csv")) + list(HYPO_OUTPUT.glob("*.csv"))
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(clean_columns, f) for f in all_files]
        
        health_deleted = 0
        hypo_deleted = 0
        
        for future in as_completed(futures):
            result = future.result()
            if len(result) == 4:
                fname, orig, new, deleted = result
                group = "Health" if any(f.name == fname and f.parent == HEALTH_OUTPUT for f in all_files) else "HYPO"
                if group == "Health":
                    health_deleted += deleted
                else:
                    hypo_deleted += deleted
                if deleted > 0:
                    log(f"  ✓ [{group}] {fname}: {orig}→{new} 列 (删 {deleted})")
    clean_time = time.time() - clean_start
    log(f"  ✓ Health 删 {health_deleted} 列, HYPO 删 {hypo_deleted} 列, 耗时: {clean_time:.1f}s")

    # 步骤5b: 院区列（住院表添加 Campus，便于后续医生检查与步骤7 静态特征）
    log("\n" + SUB)
    log("步骤1e2: 院区列（住院.csv 添加 Campus）")
    log(SUB)
    campus_start = time.time()
    for group_dir, group_name in [(HEALTH_OUTPUT, "Health"), (HYPO_OUTPUT, "HYPO")]:
        hosp_file = group_dir / "住院.csv"
        if not hosp_file.exists():
            log(f"  ⚠ [{group_name}] 住院.csv 不存在，跳过")
            continue
        try:
            df = pd.read_csv(hosp_file, low_memory=False)
            if "visit_department" not in df.columns:
                log(f"  ⚠ [{group_name}] 住院.csv 无 visit_department 列，跳过院区")
                continue
            if "Campus" in df.columns:
                log(f"  ✓ [{group_name}] 住院.csv 已有 Campus 列，跳过")
                continue
            df["Campus"] = df["visit_department"].apply(_extract_campus)
            df.to_csv(hosp_file, index=False, encoding="utf-8-sig")
            n = df["Campus"].notna().sum()
            log(f"  ✓ [{group_name}] 住院.csv: 已添加 Campus，非空 {n:,} 行")
        except Exception as e:
            log(f"  ❌ [{group_name}] 住院.csv: {e}")
    campus_time = time.time() - campus_start
    log(f"  ✓ 院区列处理耗时: {campus_time:.1f}s")

    # 步骤6: 日期8-8 规则转换（改时间，在 Study Cohort 前完成）
    log("\n" + SUB)
    log("步骤1f: 日期 8-8 规则转换")
    log(SUB)
    log("  规则: 当日 08:00-23:59→该日, 次日 00:00-07:59→前一日, 输出 YYYY-MM-DD")
    date_start = time.time()
    date_ok = 0
    date_total = 0
    for group_dir, group_name in [(HEALTH_OUTPUT, "Health"), (HYPO_OUTPUT, "HYPO")]:
        csv_files = sorted(group_dir.glob("*.csv"))
        for filepath in csv_files:
            date_total += 1
            try:
                df = pd.read_csv(filepath, low_memory=False)
                df, stats = _process_datetime_columns(df)
                if stats:
                    df.to_csv(filepath, index=False, encoding='utf-8-sig')
                    date_ok += 1
                    log(f"  ✓ [{group_name}] {filepath.name}: {len(stats)} 个时间列已转换")
            except Exception as e:
                log(f"  ❌ [{group_name}] {filepath.name}: {e}")
    date_time = time.time() - date_start
    log(f"  ✓ 转换完成: {date_ok}/{date_total} 个文件, 耗时: {date_time:.1f}s")

    # 总结
    total_time = time.time() - start_time
    log("\n" + SEP)
    log("步骤1 完成（重新开始 + 整理列 + 日期8-8）")
    log(SEP)
    log(f"  总耗时: {total_time:.1f}s (合并 {merge_time:.1f}s, admission_key {admission_time:.1f}s, 重命名 {rename_time:.1f}s, 整理列 {clean_time:.1f}s, 院区 {campus_time:.1f}s, 日期8-8 {date_time:.1f}s)")
    log("  下一步: python3 步骤2_StudyCohort筛选.py")
