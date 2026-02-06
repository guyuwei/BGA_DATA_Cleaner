#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ttkbootstrap 界面：运行数据清洗步骤 1～7，实时日志，便于医生检查。
主题与样式参考：https://github.com/israel-dryer/ttkbootstrap
"""
import sys
import subprocess
import threading
import queue
import socket
import time
import json
import os
import signal
from pathlib import Path
from tkinter import messagebox

# 单实例：新运行先关闭上次窗口
SINGLE_INSTANCE_PORT = 31955

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import ttkbootstrap as ttk
    from ttkbootstrap.constants import *
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "ttkbootstrap", "-q"], check=False)
    import ttkbootstrap as ttk
    from ttkbootstrap.constants import *


def _maybe_run_script_from_args() -> bool:
    if "--run-script" not in sys.argv:
        return False
    idx = sys.argv.index("--run-script")
    if idx + 1 >= len(sys.argv):
        return True
    script_path = sys.argv[idx + 1]
    try:
        import runpy
        runpy.run_path(script_path, run_name="__main__")
    except Exception as exc:
        print(f"[错误] 运行脚本失败: {exc}")
    return True


if _maybe_run_script_from_args():
    raise SystemExit(0)

def _get_base_dir() -> Path:
    """根据运行方式推断项目根目录（打包后为 exe/.app 的同级目录）。"""
    if getattr(sys, "frozen", False):
        exe_path = Path(sys.executable).resolve()
        if sys.platform == "darwin":
            for p in exe_path.parents:
                if p.name.endswith(".app"):
                    return p.parent
        return exe_path.parent
    return Path(__file__).resolve().parent.parent


# 工作目录：相对于 app/main.py 的父目录（项目根目录）
BASE_DIR = _get_base_dir()
DOCS_DIR = BASE_DIR / "docs"
RAW_DATA_DIR = BASE_DIR / "原始数据"
EXPECTED_RAW_SUBDIRS = [
    "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-04-06 140118_10",
    "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖2_2024-02-06 183001_21",
    "指标数据_基线数据_1家医院_分组1_低血糖预测模型-正常血糖1_2024-02-05 155459_39",
    "指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-04-06 140331_83",
    "指标数据_基线数据_1家医院_分组1_低血糖风险预测模型-低血糖_2024-02-06 183111_28",
]


def _ensure_raw_data_dirs():
    RAW_DATA_DIR.mkdir(exist_ok=True)
    for name in EXPECTED_RAW_SUBDIRS:
        (RAW_DATA_DIR / name).mkdir(parents=True, exist_ok=True)


def _raw_data_status():
    missing = []
    empty = []
    for name in EXPECTED_RAW_SUBDIRS:
        p = RAW_DATA_DIR / name
        if not p.exists():
            missing.append(name)
            continue
        if not any(p.iterdir()):
            empty.append(name)
    return missing, empty


def _open_path(path: Path):
    try:
        if sys.platform == "win32":
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.run(["open", str(path)], check=False)
        else:
            subprocess.run(["xdg-open", str(path)], check=False)
    except Exception:
        pass

# 图标所在目录（打包后为 _MEIPASS，开发时为 app/）
APP_DIR = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
LOGO_MAC = APP_DIR / "Formac.png"
LOGO_WIN = APP_DIR / "forwin.png"

# 可预览的 CSV（显示名, 相对 BASE_DIR 的路径）
CSV_PREVIEW_OPTIONS = [
    ("步骤7 - BGA 三分类大表", "BGA_时序预测大表_三分类.csv"),
    ("步骤7 - HYPO 二分类大表", "HYPO_时序预测大表_二分类.csv"),
    ("步骤7 - BGA 缺失统计", "BGA_缺失统计.csv"),
    ("Health - 住院", "Health/住院.csv"),
    ("Health - 诊断", "Health/诊断.csv"),
    ("Health - 药品医嘱", "Health/药品医嘱.csv"),
    ("Health - 非药品医嘱", "Health/非药品医嘱.csv"),
    ("Health - 生命体征", "Health/生命体征.csv"),
    ("Health - 血糖", "Health/血糖.csv"),
    ("Health - 入院记录", "Health/入院记录.csv"),
    ("Health - 糖代谢", "Health/糖代谢.csv"),
    ("Health - CRP", "Health/CRP.csv"),
    ("Health - 血常规", "Health/血常规.csv"),
    ("Health - 生化", "Health/生化.csv"),
    ("Health - 其他检验", "Health/其他检验.csv"),
    ("HYPO - 住院", "HYPO/住院.csv"),
    ("HYPO - 诊断", "HYPO/诊断.csv"),
    ("HYPO - 药品医嘱", "HYPO/药品医嘱.csv"),
    ("HYPO - 非药品医嘱", "HYPO/非药品医嘱.csv"),
    ("HYPO - 生命体征", "HYPO/生命体征.csv"),
    ("HYPO - 血糖", "HYPO/血糖.csv"),
    ("HYPO - 入院记录", "HYPO/入院记录.csv"),
    ("HYPO - 糖代谢", "HYPO/糖代谢.csv"),
    ("HYPO - CRP", "HYPO/CRP.csv"),
    ("HYPO - 血常规", "HYPO/血常规.csv"),
    ("HYPO - 生化", "HYPO/生化.csv"),
    ("HYPO - 其他检验", "HYPO/其他检验.csv"),
]
MAX_PREVIEW_ROWS = 1000
# (显示名, 脚本名) — 左侧按钮与状态栏用显示名
STEPS = [
    ("重新开始", "步骤1_重新开始.py"),
    ("Study Cohort 筛选", "步骤2_StudyCohort筛选.py"),
    ("药物医嘱整理", "步骤3_药物医嘱整理.py"),
    ("合并症提取", "步骤4_合并症提取.py"),
    ("手术事件提取", "步骤5_手术事件提取.py"),
    ("禁食营养提取", "步骤6_禁食营养提取.py"),
    ("拼接时序大表", "步骤7_拼接时序大表.py"),
]

# 状态栏说明（鼠标悬停步骤按钮时显示）
STEP_DESCRIPTIONS = {
    "重新开始": "步骤1：合并原始数据、日期 8-8 规则转换、住院表添加院区列",
    "Study Cohort 筛选": "步骤2：按诊断与科室排除患者，筛选 Study Cohort",
    "药物医嘱整理": "步骤3：药品医嘱特征提取与降糖药分类，写回药品医嘱.csv",
    "合并症提取": "步骤4：从诊断表提取合并症变量，写回诊断.csv",
    "手术事件提取": "步骤5：从非药品医嘱提取手术事件，写回非药品医嘱.csv",
    "禁食营养提取": "步骤6：从非药品医嘱提取禁食与营养，写回非药品医嘱.csv",
    "拼接时序大表": "步骤7：拼接各表生成 BGA 三分类、HYPO 二分类时序预测大表及缺失统计",
}


def _try_close_previous_instance():
    """若已有实例在运行，向其发送退出并强制关闭，确保本进程为唯一窗口。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect(("127.0.0.1", SINGLE_INSTANCE_PORT))
        s.sendall(b"quit\n")
        s.close()
        time.sleep(0.8)
        # 再次检查，若端口仍被占用则尝试强制关闭
        try:
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.settimeout(0.5)
            s2.connect(("127.0.0.1", SINGLE_INSTANCE_PORT))
            s2.close()
            # 端口仍被占用，尝试通过进程名查找并关闭
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/IM", "EHR数据清洗.exe"], 
                             capture_output=True, check=False)
            else:
                # Mac/Linux: 查找占用端口的进程并终止
                try:
                    import psutil
                    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                        try:
                            if 'main.py' in ' '.join(proc.info.get('cmdline', [])).lower() or \
                               'ehr' in proc.info.get('name', '').lower():
                                if proc.pid != os.getpid():
                                    proc.terminate()
                                    time.sleep(0.3)
                                    if proc.is_running():
                                        proc.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                except ImportError:
                    pass
        except (ConnectionRefusedError, OSError):
            pass
    except (ConnectionRefusedError, OSError):
        pass


def _run_single_instance_listener(root):
    """在后台监听端口，收到 quit 时关闭主窗口。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", SINGLE_INSTANCE_PORT))
    except OSError:
        return
    sock.listen(1)
    sock.settimeout(0.5)

    def accept_loop():
        while True:
            try:
                conn, _ = sock.accept()
                conn.recv(64)
                conn.close()
                root.after(0, root.quit)
                break
            except (socket.timeout, BlockingIOError, OSError):
                pass
            time.sleep(0.2)

    t = threading.Thread(target=accept_loop, daemon=True)
    t.start()


def _parse_original_columns() -> dict:
    """解析 最终列名汇总.txt，返回 { 'Health/住院.csv': [列名,...], ... }"""
    out = {}
    path = DOCS_DIR / "最终列名汇总.txt"
    if not path.exists():
        return out
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return out
    prefix = ""
    key = None
    for line in text.splitlines():
        line_strip = line.strip()
        if line_strip.startswith("Health 组"):
            prefix = "Health/"
            key = None
            continue
        if line_strip.startswith("HYPO 组"):
            prefix = "HYPO/"
            key = None
            continue
        if line_strip.startswith("选项:") or not line_strip:
            continue
        if "(" in line_strip and "列):" in line_strip and ".csv" in line_strip:
            # "住院.csv (16 列):"
            part = line_strip.split("(")[0].strip()
            if part.endswith(".csv"):
                key = prefix + part
                out[key] = []
            continue
        if key is not None and " . " in line:
            # "  A  . 列名" 取点号后的列名
            col_name = line.split(" . ", 1)[-1].strip()
            if col_name and not col_name.startswith("选项:"):
                out[key].append(col_name)
    return out


def _read_file(path: Path, default: str = "") -> str:
    if not path.exists():
        return f"未找到: {path}"
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return f"无法读取: {path}"


class App:
    def __init__(self):
        self.root = ttk.Window(
            title="EHR数据清洗 - 医生检查",
            themename="cosmo",
            size=(800, 560),
            resizable=(True, True),
        )
        self._set_window_icon()
        _run_single_instance_listener(self.root)
        self.log_queue = queue.Queue()
        self.process = None
        self._run_all_scripts = False
        self._run_all_index = 0
        self.step_buttons = []
        self.btn_run_all = None
        self.btn_pause = None
        self.log_text = None
        self._var_text = None
        self._rules_text = None
        self._paused = False
        self._pause_event = threading.Event()
        self._build_ui()
        self._poll_log()

    def _set_window_icon(self):
        """Mac 用鸡 logo，Windows 用蛇 logo。"""
        try:
            from tkinter import PhotoImage
            path = LOGO_MAC if sys.platform == "darwin" else LOGO_WIN
            if path.exists():
                self._icon_photo = PhotoImage(file=str(path))
                self.root.iconphoto(True, self._icon_photo)
        except Exception:
            pass

    def _build_ui(self):
        # 工作目录显示（只读）
        top = ttk.Frame(self.root, padding=5)
        top.pack(fill=X)
        ttk.Label(top, text=f"工作目录: {BASE_DIR}", bootstyle="inverse-secondary").pack(anchor=W)

        # 左右分栏：左侧步骤按钮，右侧结果标签
        content = ttk.Frame(self.root, padding=5)
        content.pack(fill=BOTH, expand=YES)

        # 左侧：步骤按钮（竖排）
        left = ttk.Labelframe(content, text="步骤", bootstyle="primary", padding=8)
        left.pack(side=LEFT, fill=Y, padx=(0, 8))
        self.btn_run_all = ttk.Button(left, text="运行全部", bootstyle="success", width=18, command=self._run_all)
        self.btn_run_all.pack(fill=X, pady=(0, 8))
        self.btn_pause = ttk.Button(left, text="暂停", bootstyle="warning", width=18, command=self._toggle_pause, state="disabled")
        self.btn_pause.pack(fill=X, pady=(0, 8))
        ttk.Separator(left, orient=HORIZONTAL).pack(fill=X, pady=4)
        self.step_buttons = []
        for name, _ in STEPS:
            b = ttk.Button(left, text=name, bootstyle="info-outline", width=18, command=lambda n=name: self._run_step(n))
            desc = STEP_DESCRIPTIONS.get(name, "")
            b.bind("<Enter>", lambda e, d=desc: self._set_status(d))
            b.bind("<Leave>", lambda e: self._set_status("就绪"))
            b.pack(fill=X, pady=2)
            self.step_buttons.append(b)

        # 右侧：结果标签页（医生视角：看“xx结果”）
        right = ttk.Frame(content)
        right.pack(side=LEFT, fill=BOTH, expand=YES)
        notebook = ttk.Notebook(right, bootstyle="primary")
        notebook.pack(fill=BOTH, expand=YES)

        # 运行结果：可切换「全部」或单个步骤的日志
        run_frame = ttk.Frame(notebook, padding=5)
        self.log_notebook = ttk.Notebook(run_frame, bootstyle="info")
        self.log_notebook.pack(fill=BOTH, expand=YES)
        self.log_text = None  # 「全部」的 Text
        self.step_log_texts = {}  # step_name -> Text
        # 「全部」
        fr_all = ttk.Frame(self.log_notebook, padding=2)
        log_all_lab = ttk.Labelframe(fr_all, text="全部步骤输出", bootstyle="info", padding=3)
        log_all_lab.pack(fill=BOTH, expand=YES)
        self.log_text = ttk.Text(log_all_lab, height=20, wrap="none", font=("Consolas", 10))
        self.log_text.pack(fill=BOTH, expand=YES)
        sb_all = ttk.Scrollbar(log_all_lab, orient=VERTICAL, command=self.log_text.yview)
        sb_all.pack(side=RIGHT, fill=Y)
        self.log_text.configure(yscrollcommand=sb_all.set)
        self.log_notebook.add(fr_all, text="全部")
        # 每个步骤单独一页
        for name, _ in STEPS:
            fr_step = ttk.Frame(self.log_notebook, padding=2)
            step_lab = ttk.Labelframe(fr_step, text=name, bootstyle="info", padding=3)
            step_lab.pack(fill=BOTH, expand=YES)
            txt = ttk.Text(step_lab, height=20, wrap="none", font=("Consolas", 10))
            txt.pack(fill=BOTH, expand=YES)
            sb_s = ttk.Scrollbar(step_lab, orient=VERTICAL, command=txt.yview)
            sb_s.pack(side=RIGHT, fill=Y)
            txt.configure(yscrollcommand=sb_s.set)
            self.step_log_texts[name] = txt
            self.log_notebook.add(fr_step, text=name)
        notebook.add(run_frame, text="运行结果")

        # 洗之前 vs 现在的列名（两列对照，无表名）
        fr2 = ttk.Frame(notebook, padding=5)
        flab = ttk.Labelframe(fr2, text="洗之前的列名 ↔ 现在的列名", bootstyle="info", padding=5)
        flab.pack(fill=BOTH, expand=YES)
        self.file_list_frame = ttk.Frame(flab)
        self.file_list_frame.pack(fill=BOTH, expand=YES)
        self.file_list_tree = ttk.Treeview(
            self.file_list_frame, columns=("洗之前的列名", "现在的列名"), show="headings", height=24
        )
        self.file_list_tree.heading("洗之前的列名", text="洗之前的列名")
        self.file_list_tree.heading("现在的列名", text="现在的列名")
        self.file_list_tree.column("洗之前的列名", width=320)
        self.file_list_tree.column("现在的列名", width=320)
        sb_f = ttk.Scrollbar(self.file_list_frame, orient=VERTICAL, command=self.file_list_tree.yview)
        sb_fh = ttk.Scrollbar(flab, orient=HORIZONTAL, command=self.file_list_tree.xview)
        self.file_list_tree.configure(yscrollcommand=sb_f.set, xscrollcommand=sb_fh.set)
        sb_f.pack(side=RIGHT, fill=Y)
        self.file_list_tree.pack(side=LEFT, fill=BOTH, expand=YES)
        sb_fh.pack(side=BOTTOM, fill=X)
        ttk.Button(fr2, text="刷新（洗前列名来自 最终列名汇总.txt，现列名来自 CSV）", bootstyle="info-outline", command=self._refresh_column_list).pack(pady=4)
        notebook.add(fr2, text="洗前与现列名")

        # 变量表结果
        var_path = DOCS_DIR / "variables.yaml"
        fr3 = ttk.Frame(notebook)
        self._var_text = ttk.Text(fr3, wrap=NONE, font=("Consolas", 9))
        self._var_text.insert(END, _read_file(var_path))
        self._var_text.config(state=DISABLED)
        sb3 = ttk.Scrollbar(fr3, orient=VERTICAL, command=self._var_text.yview)
        sb3.pack(side=RIGHT, fill=Y)
        self._var_text.pack(fill=BOTH, expand=YES)
        self._var_text.config(yscrollcommand=sb3.set)
        notebook.add(fr3, text="变量表结果")

        # 规则说明结果
        rules_path = DOCS_DIR / "rules.yaml"
        fr4 = ttk.Frame(notebook)
        self._rules_text = ttk.Text(fr4, wrap=NONE, font=("Consolas", 9))
        self._rules_text.insert(END, _read_file(rules_path))
        self._rules_text.config(state=DISABLED)
        sb4 = ttk.Scrollbar(fr4, orient=VERTICAL, command=self._rules_text.yview)
        sb4.pack(side=RIGHT, fill=Y)
        self._rules_text.pack(fill=BOTH, expand=YES)
        self._rules_text.config(yscrollcommand=sb4.set)
        notebook.add(fr4, text="规则说明结果")

        # CSV 预览
        fr_csv = ttk.Frame(notebook, padding=5)
        csv_top = ttk.Frame(fr_csv)
        csv_top.pack(fill=X, pady=(0, 5))
        ttk.Label(csv_top, text="选择 CSV:").pack(side=LEFT, padx=(0, 4))
        self.csv_combo = ttk.Combobox(
            csv_top, values=[t[0] for t in CSV_PREVIEW_OPTIONS],
            width=32, state="readonly"
        )
        self.csv_combo.pack(side=LEFT, padx=(0, 8))
        if CSV_PREVIEW_OPTIONS:
            self.csv_combo.set(CSV_PREVIEW_OPTIONS[0][0])
        ttk.Button(csv_top, text="加载预览", bootstyle="info-outline", command=self._csv_load_preview).pack(side=LEFT, padx=2)
        ttk.Button(csv_top, text="在文件夹中打开", bootstyle="secondary", command=self._csv_open_in_folder).pack(side=LEFT, padx=2)
        self.csv_tree_frame = ttk.Frame(fr_csv)
        self.csv_tree_frame.pack(fill=BOTH, expand=YES)
        self.csv_tree = None
        self.csv_vsb = None
        self.csv_hsb = None
        notebook.add(fr_csv, text="CSV 预览")

        # 状态栏
        self.status_var = ttk.StringVar(value="就绪")
        status_bar = ttk.Frame(self.root, padding=(5, 2))
        status_bar.pack(side=BOTTOM, fill=X)
        ttk.Label(status_bar, textvariable=self.status_var, bootstyle="inverse-secondary").pack(anchor=W)

        self._log("就绪。左侧点步骤运行，右侧切换标签查看对应结果。")
        self._set_status("就绪")
        # 后台自动刷新一次列名，便于检查
        if pd is not None:
            self.root.after(500, self._refresh_column_list)
        # 启动提示：原始数据目录为空时提醒用户剪切原始文件
        self.root.after(600, self._check_raw_data_prompt)

    def _log(self, msg: str, step_name: str = None):
        """step_name 不为空时，该行会同时写入「全部」和该步骤的标签页。"""
        self.log_queue.put((msg, step_name))

    def _set_status(self, text: str):
        if getattr(self, "status_var", None):
            self.status_var.set(text)

    def _poll_log(self):
        try:
            while True:
                item = self.log_queue.get_nowait()
                if isinstance(item, tuple):
                    msg, step_name = item
                else:
                    msg, step_name = item, None
                line = msg + "\n"
                if self.log_text:
                    self.log_text.insert(END, line)
                    self.log_text.see(END)
                if step_name and step_name in self.step_log_texts:
                    self.step_log_texts[step_name].insert(END, line)
                    self.step_log_texts[step_name].see(END)
        except queue.Empty:
            pass
        self.root.after(200, self._poll_log)

    def _set_buttons_enabled(self, enabled: bool):
        if self.btn_run_all:
            self.btn_run_all.config(state="normal" if enabled else "disabled")
        for b in self.step_buttons:
            b.config(state="normal" if enabled else "disabled")
        if self.btn_pause:
            self.btn_pause.config(state="normal" if not enabled else "disabled")

    def _check_raw_data_prompt(self):
        _ensure_raw_data_dirs()
        missing, empty = _raw_data_status()
        if not missing and not empty:
            return
        lines = [
            "未检测到完整的原始数据。",
            f"请将原始文件剪切到: {RAW_DATA_DIR}",
        ]
        if missing:
            lines.append("缺少目录:")
            lines.extend([f"  - {name}" for name in missing[:6]])
            if len(missing) > 6:
                lines.append(f"  ... 还有 {len(missing) - 6} 个")
        if empty:
            lines.append("空目录:")
            lines.extend([f"  - {name}" for name in empty[:6]])
            if len(empty) > 6:
                lines.append(f"  ... 还有 {len(empty) - 6} 个")
        msg = "\n".join(lines)
        open_it = messagebox.askyesno("原始数据未就绪", msg + "\n\n是否打开该文件夹？")
        if open_it:
            _open_path(RAW_DATA_DIR)

    def _toggle_pause(self):
        """暂停/继续当前运行的任务。"""
        if self._paused:
            # 继续
            self._paused = False
            self._pause_event.set()
            self.btn_pause.config(text="暂停", bootstyle="warning")
            self._set_status("已继续")
            self._log("[已继续运行]", step_name=None)
            if self.process:
                try:
                    if sys.platform != "win32":
                        # Mac/Linux: 发送 SIGCONT 恢复进程
                        self.process.send_signal(signal.SIGCONT)
                except Exception:
                    pass
        else:
            # 暂停
            self._paused = True
            self._pause_event.clear()
            self.btn_pause.config(text="继续", bootstyle="success")
            self._set_status("已暂停")
            self._log("[已暂停 - 点击「继续」恢复]", step_name=None)
            if self.process:
                try:
                    if sys.platform == "win32":
                        # Windows: 使用 taskkill 暂停（实际是终止，但可提示用户）
                        self._log("[Windows 暂停：将终止当前进程]", step_name=None)
                    else:
                        # Mac/Linux: 发送 SIGSTOP 暂停进程
                        self.process.send_signal(signal.SIGSTOP)
                except Exception:
                    pass

    def _run_all(self):
        self._set_buttons_enabled(False)
        self._run_all_scripts = True
        self._run_all_index = 0
        name, script = STEPS[0]
        self._set_status(f"正在运行：{name}")
        self._log("\n" + "=" * 60 + "\n运行全部步骤\n" + "=" * 60)
        self._log(f"\n{name}: {script}\n" + "-" * 40, step_name=name)
        threading.Thread(target=self._run_script_thread, args=(script, name), daemon=True).start()

    def _run_step(self, step_name: str):
        for name, script in STEPS:
            if name == step_name:
                self._set_buttons_enabled(False)
                self._set_status(f"正在运行：{name}")
                self._log("\n" + "=" * 60 + f"\n{name}: {script}\n" + "=" * 60, step_name=name)
                threading.Thread(target=self._run_script_thread, args=(script, name), daemon=True).start()
                return

    def _run_script_thread(self, script_name: str, step_name: str):
        script_path = BASE_DIR / script_name
        if not script_path.exists():
            self._log(f"  [错误] 未找到: {script_name}", step_name=step_name)
            self.root.after(0, lambda: self._set_buttons_enabled(True))
            return
        # 打包后：使用同一可执行文件运行步骤脚本（避免依赖系统Python）
        if getattr(sys, "frozen", False):
            cmd = [sys.executable, "--run-script", str(script_path)]
        else:
            cmd = [sys.executable, str(script_path)]
        try:
            p = subprocess.Popen(
                cmd,
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            self.process = p
            self._pause_event.set()
            while True:
                line = p.stdout.readline()
                if not line:
                    break
                # 检查暂停状态
                while self._paused:
                    self._pause_event.wait()
                    if not self._paused:
                        break
                self._log(line.rstrip(), step_name=step_name)
            p.wait()
            code = p.returncode
        except Exception as e:
            self._log(f"[异常] {e}", step_name=step_name)
            code = -1
        finally:
            self.process = None
        self.root.after(0, lambda c=code, s=step_name: self._on_finished(c, s))

    def _refresh_column_list(self):
        """刷新「洗之前的列名 ↔ 现在的列名」：洗前来自 最终列名汇总.txt，现列名来自 CSV。"""
        if pd is None:
            self._set_status("需要安装 pandas 才能刷新列名")
            return
        for i in self.file_list_tree.get_children(""):
            self.file_list_tree.delete(i)
        self._set_status("正在读取洗前列名与 CSV 表头…")

        def do_refresh():
            orig_map = _parse_original_columns()
            rows = []
            for _disp, rel in CSV_PREVIEW_OPTIONS:
                rows.append((f"——— {rel} ———", ""))
                orig_cols = orig_map.get(rel, [])
                if not (BASE_DIR / rel).exists():
                    rows.append(("(文件不存在)", ""))
                    continue
                try:
                    df = pd.read_csv(BASE_DIR / rel, nrows=0, encoding="utf-8-sig", low_memory=False)
                    current_cols = [str(c) for c in df.columns]
                except Exception as e:
                    rows.append(("", f"(读取失败: {e})"))
                    continue
                n = max(len(orig_cols), len(current_cols)) if (orig_cols or current_cols) else 0
                if n == 0:
                    rows.append(("(无列)", ""))
                for i in range(n):
                    o = orig_cols[i] if i < len(orig_cols) else ""
                    c = current_cols[i] if i < len(current_cols) else ""
                    rows.append((o, c))
            self.root.after(0, lambda: self._fill_column_list(rows))

        threading.Thread(target=do_refresh, daemon=True).start()

    def _fill_column_list(self, rows):
        for before, after in rows:
            self.file_list_tree.insert("", END, values=(before, after))
        self._set_status("就绪")

    def _csv_get_path(self):
        name = self.csv_combo.get() if self.csv_combo else ""
        for disp, rel in CSV_PREVIEW_OPTIONS:
            if disp == name:
                return BASE_DIR / rel
        return None

    def _csv_load_preview(self):
        path = self._csv_get_path()
        if not path:
            return
        if pd is None:
            self._log("[CSV 预览] 需要安装 pandas")
            return
        # 在后台线程加载大文件，避免卡界面
        def do_load():
            try:
                try:
                    df = pd.read_csv(
                        path, nrows=MAX_PREVIEW_ROWS, encoding="utf-8-sig",
                        low_memory=False, on_bad_lines="skip"
                    )
                except TypeError:
                    df = pd.read_csv(
                        path, nrows=MAX_PREVIEW_ROWS, encoding="utf-8-sig",
                        low_memory=False
                    )
                self.root.after(0, lambda: self._csv_show_table(df, path))
            except Exception as e:
                self.root.after(0, lambda: self._csv_show_error(str(e), path))
        threading.Thread(target=do_load, daemon=True).start()

    def _csv_show_error(self, err: str, path: Path):
        for w in self.csv_tree_frame.winfo_children():
            w.destroy()
        self.csv_tree = None
        self.csv_vsb = None
        self.csv_hsb = None
        lbl = ttk.Label(self.csv_tree_frame, text=f"加载失败: {path.name}\n{err}", bootstyle="danger")
        lbl.pack(expand=YES)

    def _csv_show_table(self, df: "pd.DataFrame", path: Path):
        for w in self.csv_tree_frame.winfo_children():
            w.destroy()
        cols = list(df.columns)
        if not cols:
            ttk.Label(self.csv_tree_frame, text=f"空表: {path.name}").pack(expand=YES)
            return
        self.csv_vsb = ttk.Scrollbar(self.csv_tree_frame, orient=VERTICAL)
        self.csv_hsb = ttk.Scrollbar(self.csv_tree_frame, orient=HORIZONTAL)
        self.csv_tree = ttk.Treeview(
            self.csv_tree_frame, columns=cols, show="headings",
            height=24, selectmode="extended", yscrollcommand=None, xscrollcommand=None
        )
        self.csv_vsb.config(command=self.csv_tree.yview)
        self.csv_hsb.config(command=self.csv_tree.xview)
        self.csv_tree.configure(yscrollcommand=self.csv_vsb.set, xscrollcommand=self.csv_hsb.set)
        for c in cols:
            self.csv_tree.heading(c, text=str(c)[:20])
            self.csv_tree.column(c, width=100, minwidth=60)
        for _, row in df.iterrows():
            self.csv_tree.insert("", END, values=[self._cell_str(x) for x in row])
        self.csv_vsb.pack(side=RIGHT, fill=Y)
        self.csv_hsb.pack(side=BOTTOM, fill=X)
        self.csv_tree.pack(side=LEFT, fill=BOTH, expand=YES)

    @staticmethod
    def _cell_str(x) -> str:
        if pd.isna(x):
            return ""
        s = str(x)
        return s[:80] + "…" if len(s) > 80 else s

    def _csv_open_in_folder(self):
        path = self._csv_get_path()
        if not path or not path.exists():
            return
        try:
            subprocess.run(["open", "-R", str(path)], cwd=str(BASE_DIR), check=False)
        except Exception:
            pass

    def _on_finished(self, code: int, step_name: str = None):
        self._paused = False
        self._pause_event.set()
        if self.btn_pause:
            self.btn_pause.config(text="暂停", bootstyle="warning", state="disabled")
        if self.process:
            try:
                if sys.platform != "win32":
                    # Mac/Linux: 恢复进程
                    self.process.send_signal(signal.SIGCONT)
            except Exception:
                pass
        if code != 0 and step_name:
            self._log(f"\n[进程退出] code={code}", step_name=step_name)
        elif code != 0:
            self._log(f"\n[进程退出] code={code}")
        if getattr(self, "_run_all_scripts", False) and code == 0:
            self._run_all_index += 1
            if self._run_all_index < len(STEPS):
                name, script = STEPS[self._run_all_index]
                self._set_status(f"正在运行：{name}")
                self._log("\n" + "-" * 40 + f"\n{name}: {script}\n" + "-" * 40, step_name=name)
                threading.Thread(target=self._run_script_thread, args=(script, name), daemon=True).start()
                return
            self._log("\n" + "=" * 60 + "\n全部步骤执行完毕\n" + "=" * 60)
            self._run_all_scripts = False
            self._set_status("全部步骤执行完毕")
        elif getattr(self, "_run_all_scripts", False) and code != 0:
            self._log("已中止，未继续后续步骤。")
            self._run_all_scripts = False
            self._set_status("已中止，未继续后续步骤")
        else:
            self._set_status("就绪")
        self._set_buttons_enabled(True)

    def run(self):
        self.root.mainloop()


def main():
    _try_close_previous_instance()
    app = App()
    app.run()


if __name__ == "__main__":
    main()
