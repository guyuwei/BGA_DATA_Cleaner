#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤2：Study Cohort 筛选

排除患者（诊断、科室）
"""

import pandas as pd
from pathlib import Path
import sys

SEP = "=" * 80
SUB = "-" * 60

def log(msg):
    print(msg)
    sys.stdout.flush()

log(SEP)
log("步骤2：Study Cohort 筛选")
log(SEP)

BASE_DIR = Path(__file__).resolve().parent  # 项目根目录
HEALTH_DIR = BASE_DIR / "Health"
HYPO_DIR = BASE_DIR / "HYPO"

# ============================================================================
# 第一部分：排除患者（诊断和科室）
# ============================================================================

# 定义排除条件
DIAGNOSIS_EXCLUSION = ["妊娠糖尿病", "死亡"]

DEPARTMENT_EXCLUSION = [
    "CCU病房（心血管内科）（外滩）", "CCU（月湖）",
    "EICU（月湖）", "EICU（海曙）", "监护病房（外滩）",
    "重症医学科（外滩）", "重症医学科（方桥）",
    "重症监护一（月湖）", "重症监护一（海曙）",
    "重症监护二（月湖）", "重症监护二（海曙）"
]

def find_excluded_patients(diagnosis_file):
    """从诊断文件中找出需要排除的患者"""
    log(f"\n【诊断排除】{diagnosis_file.parent.name}")
    
    df = pd.read_csv(diagnosis_file, low_memory=False)
    total_patients = df['admission_key'].nunique()
    
    log(f"  总患者数: {total_patients:,}")
    
    excluded_patients = set()
    
    for keyword in DIAGNOSIS_EXCLUSION:
        if 'disease_name' in df.columns:
            mask = df['disease_name'].astype(str).str.contains(keyword, case=False, na=False)
            patients = set(df[mask]['admission_key'].unique())
            if len(patients) > 0:
                excluded_patients.update(patients)
                log(f"  '{keyword}': {len(patients)} 个患者")
    
    log(f"  小计: {len(excluded_patients)} 个患者")
    return excluded_patients

def find_icu_patients(group_dir):
    """从所有文件中找出在ICU/CCU科室的患者"""
    log(f"\n【科室排除】{group_dir.name}")
    
    icu_patients = set()
    
    # 主要检查非药品医嘱.csv的开立科室
    files_to_check = [
        ('非药品医嘱.csv', ['prescribed_department_name']),
        ('住院.csv', ['visit_department', 'discharge_department']),
    ]
    
    found_any_dept_col = False
    
    for file_name, dept_column_names in files_to_check:
        file_path = group_dir / file_name
        if not file_path.exists():
            log(f"  ⚠️ {file_name}: 文件不存在")
            continue
        
        df = pd.read_csv(file_path, low_memory=False)
        
        # 查找实际存在的科室列
        existing_dept_cols = [col for col in dept_column_names if col in df.columns]
        
        if not existing_dept_cols:
            log(f"  ⚠️ {file_name}: 未找到科室列 (期望: {', '.join(dept_column_names)})")
            continue
        
        found_any_dept_col = True
        log(f"  检查 {file_name} ({', '.join(existing_dept_cols)})...")
        
        file_icu_patients = set()
        for dept_col in existing_dept_cols:
            for excluded_dept in DEPARTMENT_EXCLUSION:
                mask = df[dept_col].astype(str).str.contains(excluded_dept, case=False, na=False)
                patients = set(df[mask]['admission_key'].unique())
                if len(patients) > 0:
                    file_icu_patients.update(patients)
        
        if file_icu_patients:
            icu_patients.update(file_icu_patients)
            log(f"    找到 {len(file_icu_patients)} 个重症科室患者")
    
    if not found_any_dept_col:
        log(f"  ⚠️ 警告: {group_dir.name}组没有可用的科室列，无法进行科室排除")
    
    log(f"  小计: {len(icu_patients)} 个患者")
    return icu_patients

def exclude_patients_from_all_files(group_dir, excluded_patients, group_name):
    """从该组的所有文件中排除指定患者"""
    log(f"\n【删除记录】从 {group_name} 组所有文件中排除 {len(excluded_patients)} 个患者")
    
    csv_files = sorted(group_dir.glob("*.csv"))
    
    total_deleted = 0
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, low_memory=False)
            original_records = len(df)
            
            df_filtered = df[~df['admission_key'].isin(excluded_patients)]
            new_records = len(df_filtered)
            
            df_filtered.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            deleted = original_records - new_records
            total_deleted += deleted
            if deleted > 0:
                log(f"  {file_path.name}: {original_records:,} → {new_records:,} (删除 {deleted:,})")
        except Exception as e:
            log(f"  ⚠️ {file_path.name}: 跳过 ({e})")
            continue
    
    return total_deleted


# ============================================================================
# 执行流程
# ============================================================================

log("\n" + SUB)
log("排除患者")
log(SUB)

# 处理 Health 组
log("\n" + SUB)
log("Health 组")
log(SUB)

health_diagnosis = HEALTH_DIR / "诊断.csv"

if health_diagnosis.exists():
    excluded_diagnosis = find_excluded_patients(health_diagnosis)
    excluded_icu = find_icu_patients(HEALTH_DIR)
    
    all_excluded = excluded_diagnosis | excluded_icu
    
    log(f"\n  Health 组排除汇总: 诊断 {len(excluded_diagnosis)}, 科室 {len(excluded_icu)}, 总排除 {len(all_excluded)}")
    total_deleted = exclude_patients_from_all_files(HEALTH_DIR, all_excluded, "Health")
    log(f"  ✓ Health 组共删除 {total_deleted:,} 条记录")
else:
    log("  ⚠ 未找到 Health/诊断.csv")

# 处理 HYPO 组
log("\n" + SUB)
log("HYPO 组")
log(SUB)

hypo_diagnosis = HYPO_DIR / "诊断.csv"

if hypo_diagnosis.exists():
    excluded_diagnosis = find_excluded_patients(hypo_diagnosis)
    excluded_icu = find_icu_patients(HYPO_DIR)
    
    all_excluded = excluded_diagnosis | excluded_icu
    
    log(f"\n  HYPO 组排除汇总: 诊断 {len(excluded_diagnosis)}, 科室 {len(excluded_icu)}, 总排除 {len(all_excluded)}")
    total_deleted = exclude_patients_from_all_files(HYPO_DIR, all_excluded, "HYPO")
    log(f"  ✓ HYPO 组共删除 {total_deleted:,} 条记录")
else:
    log("  ⚠ 未找到 HYPO/诊断.csv")

log("\n" + SEP)
log("步骤2 完成：Study Cohort 筛选")
log(SEP)
log("  下一步: python3 步骤3_药物医嘱整理.py")
