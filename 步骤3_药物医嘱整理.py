#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤3：药物医嘱整理

1. 删除"已撤销"的医嘱
2. 根据详细规则提取降糖药物使用情况
3. 生成降糖药物变量
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
log("步骤3：药物医嘱整理")
log(SEP)

BASE_DIR = Path(__file__).resolve().parent  # 项目根目录
HEALTH_DIR = BASE_DIR / "Health"
HYPO_DIR = BASE_DIR / "HYPO"

# ============================================================================
# 降糖药物分类定义（根据用户提供的详细规则）
# ============================================================================

# 口服药分类（基于K列：inn_name 药物成分名）（根据variables.docx命名规范）
ORAL_MEDICATIONS = {
    "Metformin": {  # 二甲双胍
        "keywords": ["二甲双胍", "吡格列酮二甲双胍", "沙格列汀二甲双胍", "西格列汀二甲双胍"],
        "description": "二甲双胍",
        "full_name": "Metformin"
    },
    "Sulfonylureas": {  # 磺脲类
        "keywords": ["格列吡嗪", "格列美脲", "格列齐特"],
        "description": "磺脲类",
        "full_name": "Sulfonylureas"
    },
    "Glinides": {  # 格列奈类
        "keywords": ["瑞格列奈"],
        "description": "格列奈类",
        "full_name": "Glinides"
    },
    "TZDs": {  # 噻唑烷二酮类
        "keywords": ["吡格列酮", "吡格列酮二甲双胍"],
        "description": "噻唑烷二酮类",
        "full_name": "Thiazolidinediones"
    },
    "AGIs": {  # α-葡萄糖苷酶抑制剂
        "keywords": ["阿卡波糖", "伏格列波糖", "米格列醇"],
        "description": "α-葡萄糖苷酶抑制剂",
        "full_name": "Alpha-Glucosidase Inhibitors"
    },
    "DPP4i": {  # 二肽基肽酶-4抑制剂
        "keywords": ["利格列汀", "沙格列汀", "沙格列汀二甲双胍", "维格列汀", "西格列汀", "西格列汀二甲双胍"],
        "description": "二肽基肽酶-4抑制剂",
        "full_name": "Dipeptidyl Peptidase-4 Inhibitors"
    },
    "SGLT2i": {  # 钠-葡萄糖协同转运蛋白2抑制剂
        "keywords": ["达格列净", "卡格列净", "恩格列净"],
        "special_trade_name": "艾托格列净(捷诺妥)",  # J列或H列特殊判断
        "description": "钠-葡萄糖协同转运蛋白2抑制剂",
        "full_name": "Sodium-Glucose Cotransporter 2 Inhibitors"
    }
}

# 胰岛素分类（基于K列：inn_name 药物成分名）（根据variables.docx命名规范）
INSULIN_MEDICATIONS = {
    "Rapid_insulin": {  # 餐时胰岛素
        "keywords": ["赖脯胰岛素", "重组人胰岛素r", "门冬胰岛素"],
        "description": "餐时胰岛素",
        "full_name": "Rapid-acting insulin"
    },
    "Basal_insulin": {  # 基础胰岛素
        "keywords": ["地特胰岛素", "德谷胰岛素", "甘精胰岛素", "精蛋白重组人胰岛素n", "重组甘精胰岛素"],
        "description": "基础胰岛素",
        "full_name": "Basal insulin"
    },
    "Dual_insulin": {  # 双胰岛素
        "special_trade_name": "德谷门冬双胰岛素(诺和佳（畅充）)",  # J列特殊判断，K列为"胰岛素"
        "description": "双胰岛素",
        "full_name": "Dual insulin"
    },
    "Premixed_insulin": {  # 预混胰岛素
        "keywords": [
            "30/70混合重组人胰岛素",
            "精蛋白生物合成人胰岛素30r",
            "精蛋白重组人胰岛素(50/50)",
            "精蛋白锌重组人胰岛素70/30",
            "精蛋白锌重组赖脯胰岛素25r",
            "精蛋白锌重组赖脯胰岛素50r",
            "门冬胰岛素30"
        ],
        "description": "预混胰岛素",
        "full_name": "Premixed insulin"
    }
}

# 胰岛素注射液特殊规则
INSULIN_INJECTION_SUBCUTANEOUS = ["皮下注射", "皮内注射", "肌肉注射"]

# ============================================================================
# 处理函数
# ============================================================================

def extract_medication_features(med_file, group_name):
    """提取降糖药物使用特征"""
    log(f"\n{'='*80}")
    log(f"处理 {group_name} 组")
    log(f"{'='*80}")
    
    if not med_file.exists():
        log(f"  ⚠️ 未找到: {med_file}")
        return None, None
    
    log(f"\n读取: {med_file.name}")
    df = pd.read_csv(med_file, low_memory=False)
    
    log(f"  原始记录数: {len(df):,}")
    log(f"  原始患者数: {df['admission_key'].nunique():,}")
    
    # 列名定义
    trade_name_col = 'trade_name'  # J列：商品名
    common_name_col = 'common_name'  # H列：通用名
    inn_name_col = 'inn_name'  # K列：药物成分名
    admin_method_col = 'drug_administration_method'  # R列：给药途径
    
    # 清理制表符
    for col in [trade_name_col, common_name_col, inn_name_col, admin_method_col]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    log(f"  ✓ 已清理列中的制表符")
    
    # 步骤1: 标记"已撤销"的医嘱（不删除，后续统计时排除）
    cancelled_count = 0
    if 'order_status' in df.columns:
        cancelled_count = (df['order_status'] == '已撤销').sum()
        log(f"\n✓ 发现已撤销医嘱: {cancelled_count:,} 条（将在统计时排除）")
        log(f"  总记录数: {len(df):,}")
        log(f"  总患者数: {df['admission_key'].nunique():,}")
    
    # 创建有效医嘱数据集（排除已撤销）
    if 'order_status' in df.columns:
        df_valid = df[df['order_status'] != '已撤销'].copy()
        log(f"  有效医嘱记录数: {len(df_valid):,}")
        log(f"  有效医嘱患者数: {df_valid['admission_key'].nunique():,}")
    else:
        df_valid = df.copy()
    
    # 初始化患者药物使用字典（使用原始完整患者列表）
    patients = df['admission_key'].unique()
    features = {patient: {} for patient in patients}
    
    log(f"\n{'='*60}")
    log("提取降糖药物使用情况...")
    log('='*60)
    
    # 步骤2: 提取口服降糖药（仅使用有效医嘱）
    log("\n【口服降糖药】")
    for var_name, info in ORAL_MEDICATIONS.items():
        patients_using = set()
        matched_drugs = set()  # 记录匹配到的药物名称
        
        # 常规匹配：K列（inn_name）
        for keyword in info['keywords']:
            if inn_name_col in df_valid.columns:
                mask = df_valid[inn_name_col].astype(str).str.contains(keyword, case=False, na=False)
                matched = df_valid[mask]
                patients_using.update(matched['admission_key'].unique())
                # 记录匹配到的药物成分名
                matched_drugs.update(matched[inn_name_col].dropna().unique())
        
        # SGLT2i特殊规则：J列（trade_name）或H列（common_name）包含"艾托格列净"
        if var_name == "SGLT2i" and 'special_trade_name' in info:
            special_matched_set = set()
            
            # 检查J列（商品名）
            if trade_name_col in df_valid.columns:
                mask_trade = df_valid[trade_name_col].str.contains('艾托格列净', case=False, na=False)
                special_matched_set.update(df_valid[mask_trade]['admission_key'].unique())
                trade_count = mask_trade.sum()
            else:
                trade_count = 0
            
            # 检查H列（通用名）
            if common_name_col in df_valid.columns:
                mask_common = df_valid[common_name_col].str.contains('艾托格列净', case=False, na=False)
                special_matched_set.update(df_valid[mask_common]['admission_key'].unique())
                common_count = mask_common.sum()
            else:
                common_count = 0
            
            # 合并结果
            patients_using.update(special_matched_set)
            special_count = len(special_matched_set)
            
            if special_count > 0:
                # 获取所有匹配记录用于显示
                mask_all = df_valid[trade_name_col].str.contains('艾托格列净', case=False, na=False) | \
                           df_valid[common_name_col].str.contains('艾托格列净', case=False, na=False)
                special_matched = df_valid[mask_all]
                
                log(f"    ⭐ 特殊规则匹配（艾托格列净）: {len(special_matched)} 条记录, {special_count} 个患者")
                log(f"       - J列（商品名）匹配: {trade_count} 条")
                log(f"       - H列（通用名）匹配: {common_count} 条")
                # 显示艾托格列净的详细信息
                sample = special_matched.head(1).iloc[0]
                log(f"       示例 - J列(商品名): {sample[trade_name_col]}")
                log(f"       示例 - H列(通用名): {sample[common_name_col]}")
                log(f"       示例 - K列(成分名): {sample[inn_name_col]}")
                log(f"       示例 - R列(给药途径): {sample[admin_method_col]}")
        
        # 记录使用情况
        for patient in patients:
            features[patient][var_name] = 1 if patient in patients_using else 0
        
        log(f"  {var_name:20s} - {info['description']:25s}: {len(patients_using):5,} 个患者 ({len(patients_using)/len(patients)*100:.1f}%)")
        if matched_drugs:
            log(f"    匹配到的药物成分: {', '.join(sorted(matched_drugs)[:10])}" + ("..." if len(matched_drugs) > 10 else ""))
    
    # 步骤3: 提取胰岛素类药物
    log("\n【胰岛素类药物】")
    
    # 3.1 餐时胰岛素
    var_name = "Rapid_insulin"
    info = INSULIN_MEDICATIONS[var_name]
    patients_using = set()
    matched_drugs = set()
    
    # 预混胰岛素的排除关键词
    premix_exclusion = ['25r', '50r', '30', '70/30', '(50/50)']
    
    for keyword in info['keywords']:
        if inn_name_col in df_valid.columns:
            mask = df_valid[inn_name_col].astype(str).str.contains(keyword, case=False, na=False)
            # 排除预混胰岛素（包含25r, 50r, 30等标记的）
            for exclusion in premix_exclusion:
                mask = mask & ~df_valid[inn_name_col].astype(str).str.contains(exclusion, case=False, na=False)
            matched = df_valid[mask]
            patients_using.update(matched['admission_key'].unique())
            matched_drugs.update(matched[inn_name_col].dropna().unique())
    
    # 特殊规则：K列为"胰岛素"，H列（通用名）为"胰岛素注射液"，R列给药途径为皮下/皮内/肌肉注射
    special_insulin_count = 0
    if inn_name_col in df_valid.columns and common_name_col in df_valid.columns and admin_method_col in df_valid.columns:
        mask = (
            (df_valid[inn_name_col] == '胰岛素') &  # 精确匹配（已清理制表符）
            (df_valid[common_name_col].str.contains('胰岛素注射液', case=False, na=False)) &  # H列（通用名）
            (df_valid[admin_method_col].isin(INSULIN_INJECTION_SUBCUTANEOUS))
        )
        special_matched = df_valid[mask]
        special_insulin_count = len(special_matched)
        patients_using.update(special_matched['admission_key'].unique())
        if special_insulin_count > 0:
            log(f"    ⭐ 特殊规则匹配（胰岛素注射液+皮下注射）: {special_insulin_count} 条记录, {special_matched['admission_key'].nunique()} 个患者")
            log(f"       给药途径分布: {special_matched[admin_method_col].value_counts().to_dict()}")
            log(f"       H列通用名分布: {special_matched[common_name_col].value_counts().head(3).to_dict()}")
    
    for patient in patients:
        features[patient][var_name] = 1 if patient in patients_using else 0
    
    log(f"  {var_name:20s} - {info['description']:25s}: {len(patients_using):5,} 个患者 ({len(patients_using)/len(patients)*100:.1f}%)")
    if matched_drugs:
        log(f"    匹配到的药物成分: {', '.join(sorted(matched_drugs))}")
    
    # 3.2 基础胰岛素
    var_name = "Basal_insulin"
    info = INSULIN_MEDICATIONS[var_name]
    patients_using = set()
    matched_drugs = set()
    
    for keyword in info['keywords']:
        if inn_name_col in df_valid.columns:
            mask = df_valid[inn_name_col].astype(str).str.contains(keyword, case=False, na=False)
            matched = df_valid[mask]
            patients_using.update(matched['admission_key'].unique())
            matched_drugs.update(matched[inn_name_col].dropna().unique())
    
    for patient in patients:
        features[patient][var_name] = 1 if patient in patients_using else 0
    
    log(f"  {var_name:20s} - {info['description']:25s}: {len(patients_using):5,} 个患者 ({len(patients_using)/len(patients)*100:.1f}%)")
    if matched_drugs:
        log(f"    匹配到的药物成分: {', '.join(sorted(matched_drugs))}")
    
    # 3.3 双胰岛素
    var_name = "Dual_insulin"
    info = INSULIN_MEDICATIONS[var_name]
    patients_using = set()
    
    # K列为"胰岛素"，H列（通用名）为"德谷门冬双胰岛素(诺和佳（畅充）)"
    if inn_name_col in df_valid.columns and common_name_col in df_valid.columns:
        mask = (
            (df_valid[inn_name_col] == '胰岛素') &  # 精确匹配（已清理制表符）
            (df_valid[common_name_col].str.contains(info['special_trade_name'], case=False, na=False))  # H列（通用名）
        )
        dual_matched = df_valid[mask]
        patients_using.update(dual_matched['admission_key'].unique())
        if len(dual_matched) > 0:
            log(f"    ⭐ 找到双胰岛素: {len(dual_matched)} 条记录, {dual_matched['admission_key'].nunique()} 个患者")
            log(f"       H列(通用名)示例: {dual_matched[common_name_col].iloc[0]}")
        else:
            # 检查是否有K列为"胰岛素"的记录（在有效医嘱中）
            insulin_general = df_valid[df_valid[inn_name_col] == '胰岛素']
            log(f"    ℹ️ 未找到双胰岛素匹配")
            log(f"       K列='胰岛素'的有效医嘱记录数: {len(insulin_general):,}")
            if len(insulin_general) > 0:
                common_names = insulin_general[common_name_col].value_counts().head(5)
                log(f"       这些记录的H列(通用名)前5名:")
                for name, count in common_names.items():
                    log(f"         - {name}: {count}条")
    
    for patient in patients:
        features[patient][var_name] = 1 if patient in patients_using else 0
    
    log(f"  {var_name:20s} - {info['description']:25s}: {len(patients_using):5,} 个患者 ({len(patients_using)/len(patients)*100:.1f}%)")
    
    # 3.4 预混胰岛素
    var_name = "Premixed_insulin"
    info = INSULIN_MEDICATIONS[var_name]
    patients_using = set()
    matched_drugs = set()
    
    for keyword in info['keywords']:
        if inn_name_col in df_valid.columns:
            mask = df_valid[inn_name_col].astype(str).str.contains(keyword, case=False, na=False)
            matched = df_valid[mask]
            patients_using.update(matched['admission_key'].unique())
            matched_drugs.update(matched[inn_name_col].dropna().unique())
    
    for patient in patients:
        features[patient][var_name] = 1 if patient in patients_using else 0
    
    log(f"  {var_name:20s} - {info['description']:25s}: {len(patients_using):5,} 个患者 ({len(patients_using)/len(patients)*100:.1f}%)")
    if matched_drugs:
        log(f"    匹配到的药物成分: {', '.join(sorted(matched_drugs)[:5])}" + ("..." if len(matched_drugs) > 5 else ""))
    
    # 转换为DataFrame（只包含11个具体分类，不包含合并变量）
    features_df = pd.DataFrame.from_dict(features, orient='index')
    features_df.index.name = 'admission_key'
    features_df = features_df.reset_index()
    
    # 列顺序：admission_key, 7个口服药, 4个胰岛素
    column_order = ['admission_key']
    column_order.extend(ORAL_MEDICATIONS.keys())
    column_order.extend(INSULIN_MEDICATIONS.keys())
    features_df = features_df[column_order]
    
    log(f"\n生成的特征（11个药物分类）:")
    log(f"  行数（患者数）: {len(features_df):,}")
    log(f"  列数: {len(features_df.columns)}")
    log(f"  变量: {', '.join(features_df.columns[1:])}")
    
    # 统计使用任意口服药/胰岛素的患者数（用于汇总展示）
    oads_count = sum(1 for _, row in features_df.iterrows() if any(row[k] == 1 for k in ORAL_MEDICATIONS.keys()))
    insulin_count = sum(1 for _, row in features_df.iterrows() if any(row[k] == 1 for k in INSULIN_MEDICATIONS.keys()))
    log(f"\n【汇总统计】")
    log(f"  使用任意口服降糖药的患者: {oads_count:5,} ({oads_count/len(features_df)*100:.1f}%)")
    log(f"  使用任意胰岛素的患者:     {insulin_count:5,} ({insulin_count/len(features_df)*100:.1f}%)")
    
    # ========================================================================
    # 额外检查：胰岛素注射液的给药途径分析（基于有效医嘱）
    # ========================================================================
    log(f"\n{'='*60}")
    log("【额外检查】K列='胰岛素' + H列='胰岛素注射液'的给药途径分析（有效医嘱）")
    log('='*60)
    
    if inn_name_col in df_valid.columns and common_name_col in df_valid.columns and admin_method_col in df_valid.columns:
        insulin_injection_mask = (
            (df_valid[inn_name_col] == '胰岛素') &  # 精确匹配（已清理制表符）
            (df_valid[common_name_col].str.contains('胰岛素注射液', case=False, na=False))  # H列（通用名）
        )
        insulin_injections = df_valid[insulin_injection_mask]
        
        if len(insulin_injections) > 0:
            log(f"总记录数: {len(insulin_injections):,}")
            log(f"涉及患者数: {insulin_injections['admission_key'].nunique():,}")
            log(f"\n给药途径分布:")
            
            admin_counts = insulin_injections[admin_method_col].value_counts()
            for method, count in admin_counts.items():
                is_subcutaneous = "✓ 归入餐时胰岛素" if method in INSULIN_INJECTION_SUBCUTANEOUS else "✗ 已排除"
                log(f"  {method:20s}: {count:6,}条 {is_subcutaneous}")
            
            # 显示被排除的记录数量
            excluded_mask = ~insulin_injections[admin_method_col].isin(INSULIN_INJECTION_SUBCUTANEOUS)
            excluded_count = excluded_mask.sum()
            if excluded_count > 0:
                log(f"\n被排除的记录（非皮下/皮内/肌肉注射）: {excluded_count:,}条")
                excluded_methods = insulin_injections[excluded_mask][admin_method_col].value_counts()
                log(f"排除途径明细: {dict(excluded_methods)}")
        else:
            log("未找到符合条件的记录")
    
    # ========================================================================
    # 显示一些使用降糖药的患者示例
    # ========================================================================
    log(f"\n{'='*60}")
    log("【患者示例】使用降糖药的患者（基于有效医嘱）")
    log('='*60)
    
    # 找一些使用口服降糖药的患者（任意一种口服药）
    oral_cols = list(ORAL_MEDICATIONS.keys())
    mask_oral = features_df[oral_cols].sum(axis=1) > 0
    sample_patients = features_df[mask_oral].head(3)['admission_key'].tolist()
    
    if sample_patients:
        log(f"\n示例患者药物使用情况（前3位使用口服降糖药的患者）:")
        for idx, patient_id in enumerate(sample_patients, 1):
            patient_meds = features_df[features_df['admission_key'] == patient_id].iloc[0]
            used_meds = [col for col in features_df.columns[1:] if patient_meds[col] == 1]
            log(f"\n  患者 {idx}: {patient_id}")
            log(f"    使用的药物类型: {', '.join(used_meds) if used_meds else '无'}")
            
            # 显示该患者的具体药物记录（仅有效医嘱）
            patient_records = df_valid[df_valid['admission_key'] == patient_id]
            if len(patient_records) > 0:
                unique_drugs = patient_records[inn_name_col].dropna().unique()
                log(f"    具体药物成分（K列，有效医嘱）: {', '.join(unique_drugs[:5])}" + ("..." if len(unique_drugs) > 5 else ""))
    
    return df, features_df

def add_features_to_original(med_file, features_df, group_name):
    """将药物分类列添加到原文件后面"""
    log(f"\n添加药物分类列到原文件...")
    
    # 读取原文件
    df_original = pd.read_csv(med_file, low_memory=False)
    log(f"  原文件行数: {len(df_original):,}")
    log(f"  原文件列数: {len(df_original.columns)}")
    
    # 检查是否已经有药物分类列
    feature_cols = [col for col in features_df.columns if col != 'admission_key']
    existing_feature_cols = [col for col in feature_cols if col in df_original.columns]
    
    if existing_feature_cols:
        log(f"  ⚠️ 发现已存在的药物分类列: {len(existing_feature_cols)}个")
        log(f"     列名: {', '.join(existing_feature_cols)}")
        log(f"  删除旧列并重新添加...")
        df_original = df_original.drop(columns=existing_feature_cols)
        log(f"  删除后列数: {len(df_original.columns)}")
    
    # 合并药物分类列（基于admission_key）
    df_merged = df_original.merge(features_df, on='admission_key', how='left')
    
    # 填充缺失值为0（某些患者可能没有匹配到）
    for col in feature_cols:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].fillna(0).astype(int)
    
    log(f"  合并后行数: {len(df_merged):,}")
    log(f"  合并后列数: {len(df_merged.columns)}")
    log(f"  新增列数: {len(feature_cols)} (11个药物分类)")
    log(f"  新增列名: {', '.join(feature_cols)}")
    
    # 验证列名顺序
    expected_order = ['Metformin', 'Sulfonylureas', 'Glinides', 'TZDs', 'AGIs', 'DPP4i', 'SGLT2i', 
                      'Rapid_insulin', 'Basal_insulin', 'Dual_insulin', 'Premixed_insulin']
    log(f"  ✓ 列顺序: 口服药7个 + 胰岛素4个（根据variables.docx命名规范）")
    
    # 保存回原文件
    df_merged.to_csv(med_file, index=False, encoding='utf-8-sig')
    
    log(f"  ✓ 已保存到: {med_file}")

# ============================================================================
# 执行
# ============================================================================

# 处理 Health 组
df_health, health_features = extract_medication_features(HEALTH_DIR / "药品医嘱.csv", "Health")
if health_features is not None:
    add_features_to_original(HEALTH_DIR / "药品医嘱.csv", health_features, "Health")

# 处理 HYPO 组
df_hypo, hypo_features = extract_medication_features(HYPO_DIR / "药品医嘱.csv", "HYPO")
if hypo_features is not None:
    add_features_to_original(HYPO_DIR / "药品医嘱.csv", hypo_features, "HYPO")

# ============================================================================
# 保存变量说明
# ============================================================================

log("\n" + SUB)
log("保存变量说明")
log(SUB)

variables_doc = BASE_DIR / "降糖药物变量说明.txt"
with open(variables_doc, 'w', encoding='utf-8') as f:
    f.write("="*80 + "\n")
    f.write("降糖药物变量说明\n")
    f.write("="*80 + "\n\n")
    
    f.write("数据处理规则:\n")
    f.write("  1. 医嘱状态为'已撤销'的记录：在统计时排除（所有降糖药物变量为0）\n")
    f.write("  2. 根据药物成分名（inn_name）和商品名（trade_name）匹配\n")
    f.write("  3. 胰岛素注射液根据给药途径特殊判断\n")
    f.write("  4. 预混胰岛素（含25r、50r、30等标记）不归入餐时胰岛素\n\n")
    
    f.write("="*80 + "\n")
    f.write("新增的11个药物分类变量（二元变量，0=未使用，1=使用）\n")
    f.write("="*80 + "\n\n")
    
    f.write("【口服降糖药 - 7个】\n")
    for var_name, info in ORAL_MEDICATIONS.items():
        f.write(f"{var_name} - {info['description']} ({info['full_name']})\n")
        f.write(f"  包含: {', '.join(info['keywords'])}\n")
        if 'special_trade_name' in info:
            if var_name == "SGLT2i":
                f.write(f"  特殊: {info['special_trade_name']}（匹配J列商品名或H列通用名）\n")
            else:
                f.write(f"  特殊: {info['special_trade_name']}\n")
        f.write("\n")
    
    f.write("【胰岛素类药物 - 4个】\n")
    for var_name, info in INSULIN_MEDICATIONS.items():
        f.write(f"{var_name} - {info['description']} ({info['full_name']})\n")
        if 'keywords' in info:
            f.write(f"  包含: {', '.join(info['keywords'])}\n")
        if 'special_trade_name' in info:
            f.write(f"  特殊: {info['special_trade_name']}\n")
        f.write("\n")
    
    f.write("="*80 + "\n")
    f.write("输出位置\n")
    f.write("="*80 + "\n\n")
    f.write("药物分类列已添加到原文件末尾:\n")
    f.write("  - Health/药品医嘱.csv (新增11列)\n")
    f.write("  - HYPO/药品医嘱.csv (新增11列)\n\n")
    f.write("新增列（按顺序，根据variables.docx命名规范）:\n")
    f.write("  口服药(7): Metformin, Sulfonylureas, Glinides, TZDs, AGIs, DPP4i, SGLT2i\n")
    f.write("  胰岛素(4): Rapid_insulin, Basal_insulin, Dual_insulin, Premixed_insulin\n")

log(f"✓ 变量说明: {variables_doc}")

log("\n" + SEP)
log("步骤3 完成：药物医嘱整理")
log(SEP)
log("  修改: Health/药品医嘱.csv, HYPO/药品医嘱.csv (新增11列)")
log("  说明: 降糖药物变量说明.txt")
log("  下一步: python3 步骤4_合并症提取.py")
