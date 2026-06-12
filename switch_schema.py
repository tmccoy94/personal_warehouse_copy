"""
Warehouse schema switcher — run this from the project root.

Opens a window to select dev / test / prod, runs Set-WarehouseSchema.ps1,
waits for the containers to come back up, then opens the Dagster validation
asset in your browser so you can click Materialize to confirm.

Usage:
    python switch_schema.py
"""

import os
import subprocess
import threading
import time
import tkinter as tk
import webbrowser
from tkinter import font as tkfont

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

PROJECT_ROOT    = os.path.dirname(os.path.abspath(__file__))
ENV_FILE        = os.path.join(PROJECT_ROOT, ".env")
PS_SCRIPT       = os.path.join(PROJECT_ROOT, "Set-WarehouseSchema.ps1")
INTENT_FILE     = os.path.join(PROJECT_ROOT, "pipelines", "src", "pipelines", ".schema_intent")
DAGSTER_URL     = "http://localhost:3000"
VALIDATION_URL  = f"{DAGSTER_URL}/assets/warehouse_schema_mode_select"
DAGSTER_TIMEOUT = 180   # seconds to wait for Dagster to come back up

SCHEMAS = ["dev", "test", "prod"]

COLORS = {
    "bg":      "#1e1e2e",
    "panel":   "#2a2a3e",
    "dev":     "#4caf50",
    "test":    "#ff9800",
    "prod":    "#f44336",
    "text":    "#e0e0e0",
    "muted":   "#888888",
    "success": "#4caf50",
    "error":   "#f44336",
    "running": "#ff9800",
    "info":    "#64b5f6",
}


def get_current_schema() -> str:
    try:
        with open(ENV_FILE) as f:
            for line in f:
                if line.startswith("WAREHOUSE_SCHEMA="):
                    return line.strip().split("=", 1)[1]
    except OSError:
        pass
    return "unknown"


def _dagster_is_up() -> bool:
    if not _HAS_REQUESTS:
        return False
    try:
        r = _requests.post(
            f"{DAGSTER_URL}/graphql",
            json={"query": "{ __typename }"},
            timeout=3,
        )
        return r.status_code == 200
    except Exception:
        return False


def build_ui():
    root = tk.Tk()
    root.title("Warehouse Schema")
    root.configure(bg=COLORS["bg"])
    root.resizable(False, False)

    normal_font = tkfont.Font(family="Segoe UI", size=10)
    bold_font   = tkfont.Font(family="Segoe UI", size=10, weight="bold")
    mono_font   = tkfont.Font(family="Consolas",  size=9)
    title_font  = tkfont.Font(family="Segoe UI", size=13, weight="bold")

    # ── Title ────────────────────────────────────────────────────────────────
    tk.Label(
        root, text="Warehouse Schema", font=title_font,
        bg=COLORS["bg"], fg=COLORS["text"], pady=12,
    ).pack()

    # ── Current schema ────────────────────────────────────────────────────────
    current_var = tk.StringVar(value=f"Current:  {get_current_schema()}")
    tk.Label(
        root, textvariable=current_var, font=normal_font,
        bg=COLORS["bg"], fg=COLORS["muted"],
    ).pack(pady=(0, 12))

    # ── Schema buttons ────────────────────────────────────────────────────────
    btn_frame = tk.Frame(root, bg=COLORS["bg"])
    btn_frame.pack(padx=24, pady=(0, 16))

    buttons = []
    for schema in SCHEMAS:
        btn = tk.Button(
            btn_frame,
            text=schema.upper(),
            font=bold_font,
            bg=COLORS[schema],
            fg="white",
            activebackground=COLORS[schema],
            activeforeground="white",
            relief="flat",
            width=8,
            pady=10,
            cursor="hand2",
        )
        btn.pack(side="left", padx=6)
        buttons.append((schema, btn))

    # ── Status ────────────────────────────────────────────────────────────────
    status_var = tk.StringVar(value="Select a schema to switch.")
    status_lbl = tk.Label(
        root, textvariable=status_var, font=normal_font,
        bg=COLORS["bg"], fg=COLORS["muted"], wraplength=460,
    )
    status_lbl.pack(pady=(0, 8))

    # ── Log output ────────────────────────────────────────────────────────────
    log_frame = tk.Frame(root, bg=COLORS["panel"])
    log_frame.pack(padx=20, pady=(0, 16), fill="both")

    log = tk.Text(
        log_frame, height=10, width=58,
        bg=COLORS["panel"], fg=COLORS["text"],
        font=mono_font, relief="flat",
        state="disabled", padx=8, pady=8,
    )
    log.pack()

    # ── Open in browser button (hidden until Dagster is up) ───────────────────
    def _open_and_close():
        webbrowser.open(VALIDATION_URL)
        root.after(300, root.destroy)

    open_btn = tk.Button(
        root,
        text="Open validation asset in Dagster →",
        font=bold_font,
        bg=COLORS["info"],
        fg="white",
        activebackground=COLORS["info"],
        activeforeground="white",
        relief="flat",
        pady=8,
        cursor="hand2",
        command=_open_and_close,
    )

    def append_log(line: str):
        log.config(state="normal")
        log.insert("end", line + "\n")
        log.see("end")
        log.config(state="disabled")

    def clear_log():
        log.config(state="normal")
        log.delete("1.0", "end")
        log.config(state="disabled")

    def set_buttons_enabled(enabled: bool):
        state = "normal" if enabled else "disabled"
        for _, btn in buttons:
            btn.config(state=state)

    # ── Switch logic ──────────────────────────────────────────────────────────
    def run_switch(schema: str):
        clear_log()
        open_btn.pack_forget()
        status_var.set(f"Switching to '{schema}' — please wait...")
        status_lbl.config(fg=COLORS["running"])
        set_buttons_enabled(False)

        def execute():
            # Step 1: run the PowerShell script
            try:
                proc = subprocess.Popen(
                    [
                        "powershell", "-ExecutionPolicy", "Bypass",
                        "-File", PS_SCRIPT, schema,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    cwd=PROJECT_ROOT,
                )
                for line in proc.stdout:
                    append_log(line.rstrip())
                proc.wait()
            except Exception as exc:
                append_log(f"ERROR: {exc}")
                status_var.set("Failed to run Set-WarehouseSchema.ps1.")
                status_lbl.config(fg=COLORS["error"])
                set_buttons_enabled(True)
                return

            if proc.returncode != 0:
                status_var.set("Script failed — see output above.")
                status_lbl.config(fg=COLORS["error"])
                set_buttons_enabled(True)
                return

            # Write intent file so warehouse_mode_validation and audit_load()
            # can verify the running schema matches what was set here.
            try:
                with open(INTENT_FILE, "w", encoding="utf-8") as f:
                    f.write(schema)
                append_log(f"Schema intent recorded: {schema}")
            except OSError as exc:
                append_log(f"WARNING: could not write intent file: {exc}")

            current_var.set(f"Current:  {schema}")

            # Step 2: wait for Dagster to come back up
            append_log("")
            if _HAS_REQUESTS:
                append_log("Waiting for Dagster to come back up...")
                status_var.set("Containers restarting — waiting for Dagster...")
                status_lbl.config(fg=COLORS["running"])

                deadline = time.time() + DAGSTER_TIMEOUT
                up = False
                while time.time() < deadline:
                    if _dagster_is_up():
                        up = True
                        break
                    time.sleep(3)

                if up:
                    append_log("Dagster is up.")
                    append_log("")
                    append_log("Click the button below to open warehouse_mode_validation")
                    append_log("in the Dagster UI, then click Materialize to confirm.")
                    status_var.set(
                        f"Done — schema is '{schema}'. "
                        "Open the validation asset and click Materialize to confirm."
                    )
                    status_lbl.config(fg=COLORS["success"])
                    open_btn.pack(padx=20, pady=(0, 16), fill="x")
                else:
                    append_log(f"Dagster did not respond within {DAGSTER_TIMEOUT}s.")
                    append_log(f"Open manually: {VALIDATION_URL}")
                    status_var.set("Containers restarted but Dagster is slow to respond.")
                    status_lbl.config(fg=COLORS["running"])
                    open_btn.pack(padx=20, pady=(0, 16), fill="x")
            else:
                # requests not available — just show the link
                append_log("Tip: install the 'requests' package to enable automatic")
                append_log("Dagster health polling (pip install requests).")
                append_log(f"\nOpen manually: {VALIDATION_URL}")
                status_var.set(f"Done. Open {VALIDATION_URL} and click Materialize.")
                status_lbl.config(fg=COLORS["success"])
                open_btn.pack(padx=20, pady=(0, 16), fill="x")

            set_buttons_enabled(True)

        threading.Thread(target=execute, daemon=True).start()

    for schema, btn in buttons:
        btn.config(command=lambda s=schema: run_switch(s))

    # ── Centre on screen ──────────────────────────────────────────────────────
    root.update_idletasks()
    w, h = root.winfo_width(), root.winfo_height()
    x = (root.winfo_screenwidth()  // 2) - (w // 2)
    y = (root.winfo_screenheight() // 2) - (h // 2)
    root.geometry(f"+{x}+{y}")

    root.mainloop()


if __name__ == "__main__":
    build_ui()
