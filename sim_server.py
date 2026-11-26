#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""镁伽设备仿真台：Labillion 平台接口 + 交互口 E1 滑轨（NUC 驱动）接口 + 状态展示页。

给“通才智能体”赛马的任务1（设备接入）和任务3（软件智能体数采 B-L / B-C）做联调：
智能体像连真机一样调接口，这里按文档返回；仿真台页面显示状态、检查点和全部调用记录。

一个进程开三个端口（端口在 config.json 里改）：
  Labillion 平台接口    http://<本机IP>:8080/api/automation/v1/...     接口说明页 /docs
  交互口滑轨 NUC 驱动   http://<本机IP>:40000/api/BusFrame/System/Post  接口说明页 /docs
  仿真台（状态展示）     http://<本机IP>:8090/

依据：《第三方软件调用 Labillion 一体化通信协议文档---浦江实验室》（下称“协议文档”）、
     《Mega-Auflo-L200-2000（Linker）接口文档》（下称“NUC 文档”）、《浦江实验室软件调用流程》、
     任务1 / 任务3 检查点表、备赛协调会 PPT。文档没写清的地方怎么处理，见 详细说明.md 第 6 节。

运行：python3 sim_server.py [--demo] [--no-browser] [--config config.json] [--speed 1] [--quiet]（Windows 用 python）
     启动后自动打开状态页；页面上可以运行示例智能体、手动一步步调接口、重置到初始状态、注入故障。
     --demo：启动后直接让示例智能体把三条流程跑一遍；跑完仿真台继续开着
"""
import argparse
import asyncio
import base64
import contextlib
import datetime
import hashlib
import hmac
import json
import os
import random
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
import webbrowser
from pathlib import Path
from typing import Any, Optional

try:
    import uvicorn
    from fastapi import Depends, FastAPI, Header, Request
    from fastapi.exceptions import RequestValidationError
    from fastapi.responses import FileResponse, Response
    from fastapi.security import HTTPBearer
    from starlette.exceptions import HTTPException as StarletteHTTPException
except ImportError as missing:
    sys.exit(f"缺少依赖 {missing.name}。先在这个文件夹里运行：python3 -m pip install -r requirements.txt"
             f"（Windows 用 python）")

HERE = Path(__file__).resolve().parent
API = "/api/automation/v1"
NUC_PATH = "/api/BusFrame/System/Post"
ZERO_UUID = "00000000-0000-0000-0000-000000000000"

# NUC 结果码（8 位字符串）。文档只规定成功为 "00000000"、失败非零；下面的失败码是仿真自定的
RC_OK = "00000000"
RC_PARAM = "10000001"      # 请求参数错误
RC_DEVICE = "10000002"     # DeviceId 不匹配
RC_COMMAND = "10000003"    # 不支持的指令
RC_REJECT = "10000004"     # 当前状态不允许（ExecuteStatus=2 Reject）
RC_EVENT = "10000005"      # 重试/忽略带的 EventId 不匹配
RC_MOTION = "10008001"     # 轨道运动异常（仿真注入）
RC_ABORTED = "10008002"    # 指令被终止
RC_TIMEOUT = "10008003"    # 运动超时（仿真注入）
RC_LOST = "10008004"       # 设备失控（仿真注入，DeviceErrorAffect=1）
RC_ESTOP = "10008005"      # 急停（仿真注入，DeviceErrorAffect=2）
EXEC_SUCCESS, EXEC_ERROR, EXEC_REJECT, EXEC_ABORTED, EXEC_TIMEOUT = 0, 1, 2, 3, 4

MODE_NAMES = {"competition": "比赛规则：交互口必须空载运动", "pujiang": "浦江调用流程：滑轨载物进出"}
KIND_NAMES = {"labillion": "Labillion", "rail": "交互口", "rail_result": "交互口结果", "callback": "回调",
              "data_push": "数据推送", "platform": "平台", "physical": "物理", "sensor": "相机核验",
              "violation": "违规", "sim": "仿真台"}

# 协议文档第 8 章的 19 个接口：(内部名, 方法, 路径, 章节和名称, 请求体示例；None 表示没有请求体)
LAB_ROUTES = [
    ("login", "POST", "/login", "8.1 登录（获取 Token）",
     {"tenantId": "<config.json 里的 tenantId>", "userName": "<账号>", "password": "<密码>"}),
    ("labwaretypes", "GET", "/labwaretypes", "8.2 查询耗材类型", None),
    ("labwares", "GET", "/labwares", "8.3 查询耗材", None),
    ("load", "POST", "/lhdlabwares", "8.4 加载耗材",
     [{"labwareType": "384 Deep Well Microplate", "lidType": "Microplate Lid", "label": "sample",
       "location": "Carousel 1:Tower 2:Nest16", "barcode": "123456",
       "customAttributes": [{"name": "ztewst", "dataType": "Integer", "value": "0"}]}]),
    ("unload", "DELETE", "/lhdlabwares", "8.5 卸载耗材", ["384 Deep Well Microplate-sample[877]"]),
    ("devices", "GET", "/devices", "8.6 查询设备", None),
    ("devicestates", "GET", "/devicestates", "8.7 查询设备状态", None),
    ("online", "POST", "/onlinedevices", "8.8 上线设备", None),
    ("offline", "POST", "/offlinedevices", "8.9 下线设备", None),
    ("processdesigns", "GET", "/processdesigns", "8.10 查询流程", None),
    ("preview", "POST", "/preview", "8.11 预览",
     {"Data": [{"process": "MEGA_STEP_TEST", "iterationCount": "1"}]}),
    ("run", "POST", "/run", "8.12 运行",
     {"runName": "MEGA_STEP_TEST", "data": [{"process": "MEGA_STEP_TEST", "iterationCount": "1", "variables": []}]}),
    ("pause", "POST", "/pause", "8.13 暂停", None),
    ("continue", "POST", "/continue", "8.14 继续", None),
    ("stop", "POST", "/stop", "8.15 终止", None),
    ("workcellstate", "GET", "/workcellstate", "8.16 查询平台状态", None),
    ("errors", "GET", "/errors", "8.17 查询错误", None),
    ("warnings", "POST", "/warnings", "8.18 查询报警（文档规定 POST）", None),
    ("handleerror", "POST", "/handleerror", "8.19 容错",
     {"errorId": "35270af6-2391-45b9-a84e-e863b04b3305", "handleOption": "Retry"}),
]

# Swagger 页面上每个接口下面的说明：只写仿真的行为和文档没写清的地方
LAB_NOTES = {
    "login": "不需要 Authorization（还没有 Token）。账号见 config.json。仿真台重置后旧 Token 作废。",
    "labwares": "inUse：流程运行中、本轮加载的耗材为 \"True\"。",
    "load": "请求体是数组。location 必须是 /devices 里某台设备的 locations 之一且空着；"
            "labwareName 由平台生成：耗材类型-标签[序号]，卸载时要用它。",
    "unload": "请求体是 labwareName 字符串数组，响应 data 是对应 uuid 数组（同顺序）。名字不存在或流程运行中在用，整批不卸。",
    "online": "协议文档没写请求体。仿真：不带请求体 = 全部设备上线；也可传设备名数组。设备先 Resetting，几秒后 Idle。",
    "offline": "流程运行中（runState=Running）不能下线。请求体同上线。",
    "preview": "协议文档写的字段是 Data（类型 String）。仿真 Data / data 都认，值可以是数组或数组的 JSON 字符串。",
    "run": "runName 不给时，run.name = 流程名 + 时间戳。设备不是 Idle 时拒绝运行。流程运行时间见 config.json 的 steps。",
    "pause": "暂停后 /workcellstate 仍返回 runState=Running（文档没有 Paused 状态，评分表也要求不臆设 Paused 字段）。",
    "stop": "受理后约 stopSeconds 秒变 NotRunning，期间仍是 Running，请轮询 /workcellstate。",
    "workcellstate": "runState 只有 Running / NotRunning。run 为最近一次运行（从没运行过为 null）；"
                     "运行中 endTime 是预计结束时间。",
    "errors": "错误由仿真台页面注入。有未处理的错误时流程卡住不往下走。",
    "warnings": "协议文档写的是 POST，用 GET 会返回 405。",
    "handleerror": "handleOption 必须是该错误 handleOptions 里的一个：Retry 重做当前步骤，Ignore 跳过，Abort 中止流程。",
}

LAB_DESC = """按《第三方软件调用 Labillion 一体化通信协议文档》第 8 章实现的仿真接口。

**在本页试接口**：先调 `POST /login` 拿到 data 里的 Token → 点右上角 **Authorize** 填进去 →
每个接口的 Platform、Accept-Language 请求头已带默认值（点 Try it out 后可改）。

**判断成功**：看响应体的 `code`（业务状态码），本仿真成功时为 200（与文档各接口示例一致，可在 config.json 改成 0）。
失败时 `code` 等于 HTTP 状态码，`data` 为 null，`message` 写原因。

状态、检查点和全部调用记录在仿真台页面（默认 8090 端口）。"""

RAIL_DESC = """按《Mega-Auflo-L200-2000（Linker）接口文档》实现的交互口滑轨 NUC 驱动仿真
（设备 SBS Linear Module，型号 Auflo-L200-2000，驱动 RegripStation）。

所有指令都 POST 到同一个地址，靠 `Command` 区分：Reset 上线 / Close 下线 / Pause / Continue / Abort /
Retry / Ignore / GetState / SlideByCursorIndex（按点位索引移动）。

- `CommandSyncAsync=1` 同步：执行完一次性返回最终结果。
- `CommandSyncAsync=0` 异步：立即返回“已受理”，执行完 POST 到 `http://{客户端IP}:{回调端口}/api/OnResponse`（成功）
  或 `/api/OnError`（失败/终止）。回调端口见仿真台页面（默认 9100）。
- 点位：CursorIndex 0 = IN（收回到位），1 = OUT（弹出到位，交互位 E1），以 config.json 为准。
- GetState 的状态放在响应的 `Data` 字段（字符串）。"""


# ---------------------------------------------------------------- 小工具

def fmt_dt(ts):
    """协议文档 workcellstate 示例的时间格式：2025-12-23 17:24:13（本地时间）。"""
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""


def fmt_ms(ts):
    """记录用的时间：带毫秒和时区，方便和录像、机器人日志对时。"""
    return datetime.datetime.fromtimestamp(ts).astimezone().isoformat(timespec="milliseconds")


def is_uuid(s):
    if not isinstance(s, str):
        return False
    try:
        uuid.UUID(s)
        return True
    except ValueError:
        return False


def b64url(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def b64url_decode(s):
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def err(status, message):
    """协议文档 7.1：失败时 data 为 null。仿真的业务码 = HTTP 状态码（文档附录 A 缺失）。"""
    return status, {"code": status, "message": message, "data": None}


def http_call(method, url, payload=None, headers=None, timeout=3.0):
    """回调、数据推送、页面“手动操作”都走这里。都是局域网地址，不走系统代理；自签名 HTTPS 不校验证书。"""
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {"Content-Type": "application/json; charset=utf-8"})
    handlers = [urllib.request.ProxyHandler({})]
    if url.startswith("https://"):
        handlers.append(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()))
    try:
        with urllib.request.build_opener(*handlers).open(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def decode_for_log(raw):
    if not raw or not raw.strip():
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return raw.decode("utf-8", "replace")[:2000]


class JSONUTF8(Response):
    """协议文档 5.3：响应头必须是 application/json; charset=utf-8；中文不转义。"""
    media_type = "application/json; charset=utf-8"

    def render(self, content):
        return json.dumps(content, ensure_ascii=False).encode("utf-8")


class SimError(Exception):
    """仿真台操作（注入故障、机器人动作等）条件不满足，消息直接给页面看。"""


# ---------------------------------------------------------------- 调用记录

class EventLog:
    """本轮所有调用和状态变化，按顺序编号；同时追加写到 logs/server_events_<轮次>.jsonl。"""

    def __init__(self, logdir, quiet):
        self.logdir = logdir
        self.quiet = quiet
        self.events = []
        self.pending = set()  # 已收到请求、还没响应的事件（例如同步指令在等执行完）
        self.seq = 0
        self.round_id = None
        self.round_start = None
        self.fh = None

    def new_round(self):
        if self.fh:
            self.fh.close()
        self.events = []
        self.pending = set()
        self.round_start = time.time()
        self.round_id = datetime.datetime.fromtimestamp(self.round_start).strftime("%Y%m%d_%H%M%S")
        self.logdir.mkdir(exist_ok=True)
        self.fh = open(self.logdir / f"server_events_{self.round_id}.jsonl", "a", encoding="utf-8")

    def add(self, kind, ts=None, **fields):
        self.seq += 1
        ts = ts or time.time()
        ev = {"seq": self.seq, "round": self.round_id, "ts": ts, "time": fmt_ms(ts), "kind": kind}
        ev.update(fields)
        self.events.append(ev)
        self.pending.add(self.seq)
        return ev

    def commit(self, ev):
        """事件内容齐了（请求 + 响应）再落盘和打印。重置前发出、重置后才结束的请求不写进新一轮的文件。"""
        self.pending.discard(ev["seq"])
        if ev["round"] == self.round_id:
            self.fh.write(json.dumps(ev, ensure_ascii=False) + "\n")
            self.fh.flush()
        if not self.quiet:
            print(f"{ev['time'][11:23]} #{ev['seq']:<4} {KIND_NAMES.get(ev['kind'], ev['kind'])}  "
                  f"{ev.get('summary', '')}", flush=True)

    def since(self, seq):
        """给页面的增量：只给已完成的事件；返回的游标停在最早一个未完成事件之前，下次还会把它补上。"""
        done = [e for e in self.events if e["seq"] > seq and e["seq"] not in self.pending]
        cursor = min(self.pending) - 1 if self.pending else self.seq
        return done, max(cursor, seq)

    def note(self, kind, summary, **fields):
        ev = self.add(kind, summary=summary, **fields)
        self.commit(ev)
        return ev


# ---------------------------------------------------------------- Labillion 平台

class Labillion:
    """协议文档第 8 章的 19 个接口 + 流程运行的状态机。"""

    def __init__(self, sim):
        self.sim = sim
        self.cfg = sim.cfg["labillion"]
        self.types = {t["name"]: t for t in self.cfg["labwareTypes"]}
        self.processes = {p["name"]: p for p in self.cfg["processes"]}
        self.all_locations = [loc for d in self.cfg["devices"] for loc in d["locations"]]
        self.reset()

    def reset(self):
        self.secret = os.urandom(32)  # 每轮换签名密钥：上一轮的 Token 作废
        self.devices = [{"cfg": d, "base": "Stop", "left": 0.0, "fault": False} for d in self.cfg["devices"]]
        self.labwares = [self.make_labware(x["labwareType"], x.get("label", ""), x["location"], x.get("lidType"),
                                           x.get("barcode", ""), x.get("customAttributes", []), x["seq"])
                         for x in self.cfg.get("initialLabwares", [])]
        self.seq = int(self.cfg.get("labwareSeqStart", 1))
        self.round_loaded = set()  # 本轮通过接口加载的耗材 uuid
        self.run = None
        self.errors = []
        self.warnings = []
        self.maintenance = False
        self.inject_next = []      # 仿真台注入：接下来几次请求直接返回的 HTTP 状态码（408 / 500）
        self.last_preview = None
        self.rate = {}

    # ---- 数据视图（字段和取值格式照协议文档示例，数字和布尔值也是字符串）

    def make_labware(self, ltype, label, location, lid_type, barcode, custom, seq):
        t = self.types[ltype]
        return {"uuid": str(uuid.uuid4()), "category": t["category"],
                "labwareName": f"{ltype}-{label}[{seq}]" if label else f"{ltype}[{seq}]",
                "labwareType": ltype, "lidUuid": str(uuid.uuid4()) if lid_type else "", "label": label or "",
                "location": location, "barcode": barcode or "", "spoiled": "False", "canceled": "False",
                "inUse": "False", "isErringLabware": "False", "customAttributes": custom or []}

    def labware_view(self, lw):
        v = dict(lw)
        v["inUse"] = "True" if self.run_active() and lw["uuid"] in self.round_loaded else "False"
        return v

    def type_view(self, t):
        v = {k: t[k] for k in ("name", "category", "manufacturer", "catNo", "length", "width", "height",
                                "rowNumber", "columnNumber")}
        v["allowLidTypes"] = json.dumps(t.get("allowLidTypes", []), ensure_ascii=False, separators=(",", ":"))
        return v

    def device_view(self, d):
        c = d["cfg"]
        return {"name": c["name"], "manufacturer": c["manufacturer"], "productModel": c["productModel"],
                "type": c["type"], "locations": list(c["locations"])}

    def process_view(self, p):
        return {k: p.get(k) for k in ("name", "owner", "creationTime", "comment", "isValid", "variables")}

    def error_view(self, e):
        return {k: e[k] for k in ("uuid", "impactedIteration", "impactedDevice", "impactedLabwares",
                                  "errorMessage", "handleOptions")}

    def run_active(self):
        return self.run is not None and self.run["status"] in ("running", "stopping")

    def device_state(self, d):
        """离线 Stop → 上线中 Resetting → Idle；运行中正在干活的设备 Busy / Paused / InError（沿用 NUC 文档的状态词）。"""
        if d["fault"]:
            return "InError"
        if d["base"] != "Idle":
            return d["base"]
        r = self.run
        if self.run_active() and r["cur"] < len(r["iters"]):
            it = r["iters"][r["cur"]]
            step = it["steps"][min(it["stepIdx"], len(it["steps"]) - 1)]
            if it["startTs"] and step["device"] == d["cfg"]["name"]:
                if any(e["_device"] == step["device"] for e in self.errors):
                    return "InError"
                return "Paused" if r["paused"] else "Busy"
        return "Idle"

    def workcell_view(self):
        return {"runState": "Running" if self.run_active() else "NotRunning",
                "hasError": "True" if self.errors or any(d["fault"] for d in self.devices) else "False",
                "hasWarning": "True" if self.warnings else "False",
                "run": self.run_view() if self.run else None}

    def run_view(self):
        """run 的时间：已发生的用实际时间，没发生的用按剩余工作量推算的预计时间（协议文档示例里运行中也有 endTime）。"""
        r, now, speed = self.run, time.time(), self.sim.speed
        running = r["status"] == "running"
        t = now
        iters = []
        for i, it in enumerate(r["iters"]):
            total = sum(s["seconds"] for s in it["steps"])
            start, end = it["startTs"], it["endTs"]
            if end is None and running and i >= r["cur"]:
                done = (sum(s["seconds"] for s in it["steps"][:it["stepIdx"]]) + it["stepDone"]) if start else 0.0
                start = start or t
                end = t + (total - done) / speed
                t = end
            iters.append({"process": it["process"], "iterationId": it["iterationId"],
                          "startTime": fmt_dt(start), "endTime": fmt_dt(end)})
        if r["endTs"]:
            end_all = r["endTs"]
        elif running:
            end_all = t
        else:  # 终止中
            end_all = now + max(r["stopLeft"], 0) / speed
        return {"name": r["name"], "startTime": fmt_dt(r["startTs"]), "endTime": fmt_dt(end_all), "iterations": iters}

    def process_seconds(self, name):
        return sum(s["seconds"] for s in self.processes[name]["steps"])

    # ---- 公共检查（协议文档 6.1 请求头、7.2 状态码）

    def handle(self, key, method, headers, raw, client):
        """返回 (HTTP 状态码, 响应体, 规范提示列表)。"""
        hints = []
        if self.maintenance:
            return (*err(503, "Labillion 维护中，服务暂不可用（仿真台打开了 503 维护模式）"), hints)
        if self.inject_next:
            code = self.inject_next.pop(0)
            why = "请求超时：数据中台没有及时收到请求" if code == 408 else "服务端内部错误"
            return (*err(code, f"{why}（仿真台注入，协议文档 7.2.1）"), hints)
        now = time.time()
        win = self.rate.get(client)
        if not win or now - win[0] >= 1.0:
            win = self.rate[client] = [now, 0]
        win[1] += 1
        limit = int(self.cfg.get("rateLimitPerSecond", 0))
        if limit and win[1] > limit:
            return (*err(429, f"请求过于频繁：同一客户端每秒超过 {limit} 次，请降低轮询频率"), hints)

        ctype = headers.get("content-type", "")
        has_body = bool(raw.strip())
        if has_body and "application/json" not in ctype.lower():
            return (*err(400, "请求体必须是 JSON，Content-Type 应为 application/json（协议文档 5.4）"), hints)
        if not ctype:
            hints.append("缺少 Content-Type 请求头：协议文档要求每个请求都带 Content-Type: application/json")
        if not headers.get("accept-language"):
            if self.sim.settings["strictHeaders"]:
                return (*err(400, "缺少 Accept-Language 请求头（协议文档各接口均为必填，例如 zh-CN）"), hints)
            hints.append("缺少 Accept-Language 请求头（协议文档必填）")
        platform = headers.get("platform")
        if not platform:
            return (*err(400, "缺少 Platform 请求头（平台唯一标识，协议文档 6.1）"), hints)
        if platform != self.cfg["platform"]:
            return (*err(403, f"Platform 不匹配：收到 {platform}，本平台是 {self.cfg['platform']}"), hints)
        if key != "login":
            problem = self.check_token(headers.get("authorization"))
            if problem:
                return (*err(401, problem), hints)
        body = None
        if has_body and method != "GET":
            try:
                body = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as e:
                return (*err(400, f"请求体不是合法的 UTF-8 JSON：{e}"), hints)
        status, payload = getattr(self, "api_" + key)(body)
        return status, payload, hints

    def check_token(self, auth):
        if not auth:
            return "缺少 Authorization 请求头：格式为 Bearer + 空格 + Token，Token 由 /login 获取"
        if not auth.startswith("Bearer "):
            return "Authorization 格式不对：应为 Bearer + 空格 + Token"
        token = auth[7:].strip()
        try:
            head, body, sig = token.split(".")
            expect = b64url(hmac.new(self.secret, f"{head}.{body}".encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(expect, sig):
                return "Token 无效（仿真台重置后旧 Token 作废，请重新登录）"
            payload = json.loads(b64url_decode(body))
        except (ValueError, TypeError):
            return "Token 无效：不是本平台签发的 Token"
        if payload.get("exp", 0) < time.time():
            return "Token 已过期，请重新登录"
        return None

    def ok(self, data=None):
        return 200, {"code": self.sim.settings["successCode"], "message": "请求成功", "data": data}

    def ok_task(self, task_id):
        """协议文档 8.11 / 8.12：预览和运行的响应字段是 code、message、taskId（没有 data）。"""
        return 200, {"code": self.sim.settings["successCode"], "message": "请求成功", "taskId": task_id}

    # ---- 19 个接口

    def api_login(self, body):
        if not isinstance(body, dict):
            return err(400, "请求体应为 JSON 对象：{\"tenantId\", \"userName\", \"password\"}")
        bad = [k for k in ("tenantId", "userName", "password") if not isinstance(body.get(k), str) or not body[k]]
        if bad:
            return err(400, f"缺少字段或不是字符串：{', '.join(bad)}")
        users = {(u["userName"], u["password"]) for u in self.cfg["users"]}
        if body["tenantId"] != self.cfg["tenantId"] or (body["userName"], body["password"]) not in users:
            return err(401, "租户、用户名或密码错误")
        now = int(time.time())
        payload = {"tenantId": body["tenantId"], "userName": body["userName"], "platform": self.cfg["platform"],
                   "iat": now, "exp": now + int(self.cfg.get("tokenTtlMinutes", 120)) * 60, "jti": str(uuid.uuid4())}
        head = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
        part = b64url(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode())
        sig = b64url(hmac.new(self.secret, f"{head}.{part}".encode(), hashlib.sha256).digest())
        return self.ok(f"{head}.{part}.{sig}")

    def api_labwaretypes(self, body):
        return self.ok([self.type_view(t) for t in self.cfg["labwareTypes"]])

    def api_labwares(self, body):
        return self.ok([self.labware_view(lw) for lw in self.labwares])

    def api_load(self, body):
        if not isinstance(body, list) or not body:
            return err(400, "请求体应为非空数组，每项一个耗材：[{\"labwareType\", \"location\", ...}]（协议文档 8.4）")
        occupied = {lw["location"] for lw in self.labwares}
        problems, seen = [], set()
        for i, item in enumerate(body, 1):
            if not isinstance(item, dict):
                problems.append(f"第 {i} 项不是 JSON 对象")
                continue
            lt, loc, lid = item.get("labwareType"), item.get("location"), item.get("lidType")
            if not isinstance(lt, str) or lt not in self.types:
                problems.append(f"第 {i} 项 labwareType 不存在：{lt}（可用类型见 /labwaretypes）")
            if not isinstance(loc, str) or loc not in self.all_locations:
                problems.append(f"第 {i} 项 location 不存在：{loc}（可用位置见 /devices 的 locations）")
            elif loc in occupied or loc in seen:
                problems.append(f"第 {i} 项 location 已有耗材：{loc}")
            else:
                seen.add(loc)
            for k in ("label", "barcode", "lidType"):
                if item.get(k) is not None and not isinstance(item.get(k), str):
                    problems.append(f"第 {i} 项 {k} 应为字符串")
            if isinstance(lid, str) and lid and isinstance(lt, str) and lt in self.types:
                allowed = self.types[lt].get("allowLidTypes", [])
                if lid not in allowed:
                    problems.append(f"第 {i} 项 lidType={lid} 不在该耗材允许的盖子类型里：{allowed}")
            ca = item.get("customAttributes")
            if ca is not None and not (isinstance(ca, list) and all(isinstance(a, dict) for a in ca)):
                problems.append(f"第 {i} 项 customAttributes 应为对象数组 [{{name, dataType, value}}]")
        if problems:
            return err(400, "；".join(problems))
        created = []
        for item in body:
            lw = self.make_labware(item["labwareType"], item.get("label") or "", item["location"],
                                   item.get("lidType"), item.get("barcode") or "",
                                   item.get("customAttributes") or [], self.seq)
            self.seq += 1
            self.labwares.append(lw)
            self.round_loaded.add(lw["uuid"])
            created.append(lw)
        self.sim.scene.on_labware_loaded(created)
        return self.ok([self.labware_view(lw) for lw in created])

    def api_unload(self, body):
        if not isinstance(body, list) or not body or not all(isinstance(n, str) for n in body):
            return err(400, "请求体应为 labwareName 字符串数组，例如 [\"384 Deep Well Microplate-plate[879]\"]（协议文档 8.5）")
        if len(set(body)) != len(body):
            return err(400, "请求体里有重复的 labwareName")
        by_name = {lw["labwareName"]: lw for lw in self.labwares}
        missing = [n for n in body if n not in by_name]
        if missing:
            return err(400, f"耗材不存在：{'、'.join(missing)}（以 /labwares 返回的 labwareName 为准）")
        busy = [n for n in body if self.labware_view(by_name[n])["inUse"] == "True"]
        if busy:
            return err(400, f"耗材正在使用中（流程运行中）：{'、'.join(busy)}；先终止流程或等流程结束")
        removed = [by_name[n] for n in body]
        gone = {lw["uuid"] for lw in removed}
        self.labwares = [lw for lw in self.labwares if lw["uuid"] not in gone]
        self.sim.scene.on_labware_unloaded(removed)
        return self.ok([lw["uuid"] for lw in removed])

    def api_devices(self, body):
        return self.ok([self.device_view(d) for d in self.devices])

    def api_devicestates(self, body):
        return self.ok([{"deviceName": d["cfg"]["name"], "deviceState": self.device_state(d)} for d in self.devices])

    def target_devices(self, body):
        """上线/下线的请求体协议文档没写。仿真：不带请求体 = 全部设备；带设备名数组 = 只处理这些。"""
        if body in (None, [], {}):
            return self.devices, None
        if isinstance(body, list) and all(isinstance(n, str) for n in body):
            by_name = {d["cfg"]["name"]: d for d in self.devices}
            missing = [n for n in body if n not in by_name]
            if missing:
                return None, f"设备不存在：{'、'.join(missing)}（设备名见 /devices）"
            return [by_name[n] for n in body], None
        return None, "请求体协议文档未定义；仿真接受不带请求体（全部设备）或设备名字符串数组"

    def api_online(self, body):
        targets, problem = self.target_devices(body)
        if problem:
            return err(400, problem)
        for d in targets:
            if d["base"] == "Stop":
                d["base"], d["left"] = "Resetting", float(self.cfg.get("onlineSeconds", 3))
        return self.ok(None)

    def api_offline(self, body):
        if self.run_active():
            return err(400, "流程运行中（runState=Running），不能下线设备；先终止或等流程结束")
        targets, problem = self.target_devices(body)
        if problem:
            return err(400, problem)
        for d in targets:
            d["base"], d["left"] = "Stop", 0.0
        return self.ok(None)

    def api_processdesigns(self, body):
        return self.ok([self.process_view(p) for p in self.cfg["processes"]])

    def parse_entries(self, body, fields):
        """预览用 Data（协议文档 8.11，类型写的 String），运行用 data（8.12，Object）。两种写法都认。"""
        if not isinstance(body, dict):
            return None, "请求体应为 JSON 对象"
        raw = next((body[f] for f in fields if f in body), None)
        if raw is None:
            return None, f"缺少运行参数字段 {fields[0]}"
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                return None, f"{fields[0]} 是字符串时必须是 JSON 数组的字符串"
        if not isinstance(raw, list) or not raw:
            return None, "运行参数应为非空数组：[{\"process\": 流程名, \"iterationCount\": \"通量数\"}]"
        entries, problems = [], []
        for i, e in enumerate(raw, 1):
            if not isinstance(e, dict):
                problems.append(f"第 {i} 项不是 JSON 对象")
                continue
            name = e.get("process")
            p = self.processes.get(name) if isinstance(name, str) else None
            if p is None:
                problems.append(f"第 {i} 项流程不存在：{name}（可用流程见 /processdesigns）")
                continue
            if p.get("isValid") != "True":
                problems.append(f"第 {i} 项流程不合法（isValid=False）：{name}")
                continue
            cnt = e.get("iterationCount", "1")
            n = None
            if isinstance(cnt, (int, str)) and not isinstance(cnt, bool):
                try:
                    n = int(cnt)
                except ValueError:
                    n = None
            if n is None or not 1 <= n <= 999:
                problems.append(f"第 {i} 项 iterationCount 应为 1~999 的整数（文档示例写成字符串 \"2\"）：{cnt}")
                continue
            if e.get("variables") is not None and not isinstance(e.get("variables"), list):
                problems.append(f"第 {i} 项 variables 应为数组")
                continue
            entries.append({"process": name, "count": n, "variables": e.get("variables")})
        if problems:
            return None, "；".join(problems)
        return entries, None

    def api_preview(self, body):
        entries, problem = self.parse_entries(body, ("Data", "data"))
        if problem:
            return err(400, problem)
        task_id = str(uuid.uuid4())
        secs = sum(self.process_seconds(e["process"]) * e["count"] for e in entries)
        self.last_preview = {"taskId": task_id, "entries": entries, "seconds": secs, "time": fmt_ms(time.time())}
        return self.ok_task(task_id)

    def api_run(self, body):
        entries, problem = self.parse_entries(body, ("data", "Data"))
        if problem:
            return err(400, problem)
        run_name = body.get("runName")
        if run_name is not None and not isinstance(run_name, str):
            return err(400, "runName 应为字符串")
        if self.run_active():
            return err(400, "平台正在运行（runState=Running），不能重复启动")
        needed = []
        for e in entries:
            for s in self.processes[e["process"]]["steps"]:
                if s["device"] not in needed:
                    needed.append(s["device"])
        states = {d["cfg"]["name"]: self.device_state(d) for d in self.devices}
        not_ready = [f"{n}={states.get(n, '不存在')}" for n in needed if states.get(n) != "Idle"]
        if not_ready:
            return err(400, f"设备未就绪：{'、'.join(not_ready)}（先调 /onlinedevices，再查 /devicestates 等到 Idle）")
        now = time.time()
        name = run_name or entries[0]["process"] + datetime.datetime.fromtimestamp(now).strftime("%Y%m%d%H%M%S")
        iters = []
        for e in entries:
            for _ in range(e["count"]):
                iters.append({"process": e["process"], "iterationId": str(len(iters) + 1),
                              "steps": self.processes[e["process"]]["steps"], "stepIdx": 0, "stepDone": 0.0,
                              "startTs": None, "endTs": None})
        iters[0]["startTs"] = now
        self.run = {"taskId": str(uuid.uuid4()), "name": name, "status": "running", "paused": False,
                    "startTs": now, "endTs": None, "iters": iters, "cur": 0, "stopLeft": 0.0, "stopReason": None,
                    "labwares": [lw["labwareName"] for lw in self.labwares if lw["uuid"] in self.round_loaded],
                    "dataPush": None}
        return self.ok_task(self.run["taskId"])

    def api_pause(self, body):
        r = self.run
        if not self.run_active():
            return err(400, "当前没有运行中的流程（runState=NotRunning），无法暂停")
        if r["status"] == "stopping":
            return err(400, "流程正在终止，无法暂停")
        if r["paused"]:
            return err(400, "流程已经处于暂停中")
        r["paused"] = True
        return self.ok(None)

    def api_continue(self, body):
        r = self.run
        if not self.run_active():
            return err(400, "当前没有运行中的流程（runState=NotRunning），无法继续")
        if r["status"] == "stopping":
            return err(400, "流程正在终止，无法继续")
        if not r["paused"]:
            return err(400, "流程没有暂停，不需要继续")
        r["paused"] = False
        return self.ok(None)

    def api_stop(self, body):
        r = self.run
        if not self.run_active():
            return err(400, "当前没有运行中的流程（runState=NotRunning），无法终止")
        if r["status"] == "stopping":
            return err(400, "流程正在终止中，请轮询 /workcellstate 等 NotRunning")
        r["status"], r["stopReason"], r["stopLeft"] = "stopping", "stopped", float(self.cfg.get("stopSeconds", 2))
        return self.ok(None)

    def api_workcellstate(self, body):
        return self.ok(self.workcell_view())

    def api_errors(self, body):
        return self.ok([self.error_view(e) for e in self.errors])

    def api_warnings(self, body):
        return self.ok([dict(w) for w in self.warnings])

    def api_handleerror(self, body):
        if not isinstance(body, dict):
            return err(400, "请求体应为 JSON 对象：{\"errorId\", \"handleOption\"}")
        eid, opt = body.get("errorId"), body.get("handleOption")
        if not isinstance(eid, str) or not isinstance(opt, str):
            return err(400, "errorId 和 handleOption 都必填（字符串）")
        e = next((x for x in self.errors if x["uuid"] == eid), None)
        if e is None:
            return err(400, f"错误不存在或已处理：{eid}（先查 /errors）")
        options = json.loads(e["handleOptions"])
        if opt not in options:
            return err(400, f"该错误不支持 {opt}，可选：{options}")
        self.errors.remove(e)
        r = self.run
        if self.run_active():
            it = r["iters"][r["cur"]]
            if opt == "Retry":
                it["stepDone"] = 0.0
            elif opt == "Ignore":
                it["stepDone"] = float(it["steps"][it["stepIdx"]]["seconds"])
            elif opt == "Abort" and r["status"] == "running":
                r["status"], r["stopReason"], r["stopLeft"] = "stopping", "aborted", float(self.cfg.get("stopSeconds", 2))
        return self.ok(None)

    # ---- 时间推进

    def tick(self, now, dt):
        ready = []
        for d in self.devices:
            if d["base"] == "Resetting":
                d["left"] -= dt
                if d["left"] <= 0:
                    d["base"] = "Idle"
                    ready.append(d["cfg"]["name"])
        if ready:
            self.sim.log.note("platform", f"设备上线完成，状态 Idle：{'、'.join(ready)}")
        r = self.run
        if not r:
            return
        if r["status"] == "stopping":
            r["stopLeft"] -= dt
            if r["stopLeft"] <= 0:
                self.finish(r["stopReason"])
            return
        if r["status"] != "running" or r["paused"]:
            return
        cur = r["iters"][r["cur"]]
        dev = cur["steps"][cur["stepIdx"]]["device"]
        if not self.errors and any(d["fault"] and d["cfg"]["name"] == dev for d in self.devices):
            # 当前步骤用的设备故障（InError）：生成一条错误，流程卡住，等 /handleerror（故障没解除时 Retry 会再报）
            e = self.inject_error(f"设备故障：{dev} 处于 InError", ["Retry", "Abort"])
            self.sim.log.note("platform", f"流程卡在 {e['impactedDevice']}：设备故障，等待容错处理")
        if self.errors:
            return
        work = dt
        while work > 1e-9 and r["status"] == "running":
            it = r["iters"][r["cur"]]
            if it["startTs"] is None:
                it["startTs"] = now
            step = it["steps"][it["stepIdx"]]
            use = min(work, step["seconds"] - it["stepDone"])
            it["stepDone"] += use
            work -= use
            if it["stepDone"] >= step["seconds"] - 1e-9:
                it["stepIdx"] += 1
                it["stepDone"] = 0.0
                if it["stepIdx"] >= len(it["steps"]):
                    it["stepIdx"] = len(it["steps"]) - 1
                    it["stepDone"] = float(step["seconds"])
                    it["endTs"] = now
                    r["cur"] += 1
                    if r["cur"] >= len(r["iters"]):
                        self.finish("completed")

    def finish(self, reason):
        r, now = self.run, time.time()
        r["status"], r["endTs"], r["paused"] = reason, now, False
        for it in r["iters"]:
            if it["startTs"] and not it["endTs"]:
                it["endTs"] = now
        r["cur"] = min(r["cur"], len(r["iters"]) - 1)
        self.errors = []
        text = {"completed": "正常完成", "stopped": "被终止（/stop，不计实验正常完成）",
                "aborted": "被中止（容错选 Abort）"}[reason]
        self.sim.log.note("platform", f"流程结束：{r['name']} {text}，runState 变为 NotRunning",
                          runName=r["name"], endReason=reason)
        if reason == "completed":
            self.push_data()

    def push_data(self):
        """流程图：“设备运行完成是否有数据 → 是 → Labillion 调用具身接收数据接口”。接口格式文档没定义，这里是占位格式。"""
        r = self.run
        url = self.sim.settings.get("dataPushUrl") or ""
        has_data = any(self.processes[it["process"]].get("producesData") for it in r["iters"])
        if not has_data:
            r["dataPush"] = "流程没有数据"
            return
        if not url:
            r["dataPush"] = "有数据，但没配置具身接收数据接口地址（仿真台设置 dataPushUrl），未推送"
            self.sim.log.note("data_push", r["dataPush"])
            return
        payload = {"runName": r["name"], "taskId": r["taskId"], "startTime": fmt_dt(r["startTs"]),
                   "endTime": fmt_dt(r["endTs"]),
                   "results": [{"iterationId": it["iterationId"], "process": it["process"],
                                "labwares": r["labwares"], "value": round(random.uniform(0.1, 2.0), 4)}
                               for it in r["iters"]],
                   "_note": "仿真占位格式：协议文档没有定义“具身接收数据接口”，正式格式以镁伽提供为准"}
        r["dataPush"] = f"已推送到 {url}"
        self.sim.post_json_later("data_push", url, payload, f"推送运行数据 {r['name']}")

    # ---- 仿真台注入

    def inject_error(self, message, options):
        r = self.run
        if not (self.run_active() and r["status"] == "running"):
            raise SimError("没有正在运行的流程，无法注入流程错误（先让智能体 /run）")
        it = r["iters"][r["cur"]]
        step = it["steps"][it["stepIdx"]]
        opts = [o for o in (options or ["Retry", "Ignore", "Abort"]) if o in ("Retry", "Ignore", "Abort")]
        e = {"uuid": str(uuid.uuid4()), "impactedIteration": it["iterationId"],
             "impactedDevice": f"{it['process']}[{it['iterationId']}]-{step['device']}-{step['action']}",
             "impactedLabwares": ", ".join(r["labwares"]), "errorMessage": message or "任务无法正常启动",
             "handleOptions": json.dumps(opts or ["Retry"], separators=(",", ":")), "_device": step["device"]}
        self.errors.append(e)
        return e

    def internal_view(self):
        """只给仿真台页面看的内部状态（接口不返回）。"""
        r = self.run
        if not r:
            return None
        it = r["iters"][min(r["cur"], len(r["iters"]) - 1)]
        step = it["steps"][it["stepIdx"]]
        total = sum(self.process_seconds(x["process"]) for x in r["iters"])
        done = sum(sum(s["seconds"] for s in x["steps"]) for x in r["iters"] if x["endTs"] and r["status"] == "running")
        if r["status"] == "running":
            done += sum(s["seconds"] for s in it["steps"][:it["stepIdx"]]) + it["stepDone"]
        active = r["status"] in ("running", "stopping")

        def iter_state(i, x):  # 每个通量：done 完成 / active 进行中 / pending 等待 / stopped 被终止 / skipped 没执行
            if r["status"] == "completed" or i < r["cur"]:
                return "done"
            if i > r["cur"] or not x["startTs"]:
                return "pending" if active else "skipped"
            return "active" if active else "stopped"
        return {"name": r["name"], "taskId": r["taskId"], "status": r["status"], "paused": r["paused"],
                "blockedByError": bool(self.errors), "iteration": it["iterationId"], "iterations": len(r["iters"]),
                "iterStates": [iter_state(i, x) for i, x in enumerate(r["iters"])],
                "process": it["process"], "step": f"{step['device']} · {step['action']}",
                "stepProgress": round(it["stepDone"] / step["seconds"], 3) if step["seconds"] else 1,
                "progress": round(done / total, 3) if total and r["status"] == "running" else
                (1.0 if r["status"] == "completed" else None),
                "dataPush": r["dataPush"]}

    def doc_examples(self):
        """Swagger 页面上的响应示例（用当前配置生成，和真实返回同格式）。"""
        sc = self.sim.settings["successCode"]

        def ok(d):
            return {"code": sc, "message": "请求成功", "data": d}
        lw = self.make_labware("384 Deep Well Microplate", "sample", "Carousel 1:Tower 2:Nest16", "Microplate Lid",
                               "123456", [{"name": "ztewst", "dataType": "Integer", "value": "0"}], 877)
        now = time.time()
        wc = {"runState": "Running", "hasError": "False", "hasWarning": "False",
              "run": {"name": "MEGA_STEP_TEST", "startTime": fmt_dt(now), "endTime": fmt_dt(now + 100),
                      "iterations": [{"process": "MEGA_STEP_TEST", "iterationId": "1",
                                      "startTime": fmt_dt(now), "endTime": fmt_dt(now + 100)}]}}
        task = {"code": sc, "message": "请求成功", "taskId": str(uuid.uuid4())}
        return {
            "login": ok("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRJZCI6...（省略）"),
            "labwaretypes": ok([self.type_view(t) for t in self.cfg["labwareTypes"]]),
            "labwares": ok([self.labware_view(x) for x in self.labwares]),
            "load": ok([lw]), "unload": ok([lw["uuid"]]),
            "devices": ok([self.device_view(d) for d in self.devices]),
            "devicestates": ok([{"deviceName": d["cfg"]["name"], "deviceState": "Idle"} for d in self.devices]),
            "online": ok(None), "offline": ok(None), "pause": ok(None), "continue": ok(None), "stop": ok(None),
            "handleerror": ok(None), "processdesigns": ok([self.process_view(p) for p in self.cfg["processes"]]),
            "preview": task, "run": task, "workcellstate": ok(wc),
            "errors": ok([{"uuid": str(uuid.uuid4()), "impactedIteration": "1",
                           "impactedDevice": "MEGA_STEP_TEST[1]-智昌机械臂-Move Labware",
                           "impactedLabwares": lw["labwareName"], "errorMessage": "任务无法正常启动",
                           "handleOptions": "[\"Retry\",\"Ignore\",\"Abort\"]"}]),
            "warnings": ok([{"uuid": str(uuid.uuid4()), "impactedDevice": "高内涵", "impactedLabwares": "",
                             "warningMessage": "报警: 仿真注入"}]),
        }


# ---------------------------------------------------------------- 交互口滑轨（NUC 驱动）

class Rail:
    """SBS Linear Module（Auflo-L200-2000，驱动 RegripStation）的 NUC 驱动：NUC 文档第 4~7 章。

    状态机照 NUC 文档 7.2.8 的状态转换图：Stop →(Reset) Resetting →(完成) Idle →(新指令) Busy →(完成/Abort) Idle；
    Idle/Busy →(Pause) Paused →(Continue) 回原状态；出错进 InError，Retry / Ignore 处理；除 Resetting 外都能 Reset。
    """

    COMMANDS = ("Reset", "Close", "Pause", "Continue", "Abort", "Retry", "Ignore", "GetState", "SlideByCursorIndex")

    def __init__(self, sim):
        self.sim = sim
        self.cfg = sim.cfg["rail"]
        self.positions = {int(p["cursorIndex"]): p for p in self.cfg["positions"]}
        self.task = None
        self.reset()

    def reset(self):
        if self.task:
            self.end_task(self.task, EXEC_ABORTED, RC_ABORTED, "仿真台重置，指令终止")
        self.state = "Stop"
        self.pos = float(self.positions[int(self.cfg["homeIndex"])]["mm"])
        self.paused_from = None
        self.error = None          # InError 时待处理的错误：{eventId, task}
        self.moving = False
        self.settle_left = 0.0     # 停下后还在晃的时间（毫秒，仿真时间）
        self.was_static = True
        self.fault = None          # 仿真台给“下一次移动”设的故障：error / timeout / offset
        self.reset_left = 0.0
        self.seen_ids = set()
        self.sensor_reported = None
        self.last = None           # 最近一次指令的最终结果（给设备界面“实况”页看）
        self.motion = getattr(self, "motion", 0)  # 第几次移动类指令；到位证据、核验都按这个编号对到具体哪一次移动

    # ---- 物理量（给仿真台和“仿真相机”用，NUC 接口里没有）

    def at_name(self):
        tol = float(self.cfg.get("arrivalToleranceMm", 1.0))
        for p in self.positions.values():
            if abs(self.pos - float(p["mm"])) <= tol:
                return p["name"]
        return None

    def is_static(self):
        return not self.moving and self.settle_left <= 0

    def index_of(self, name):
        return next((i for i, p in self.positions.items() if p["name"] == name), None)

    def sensor(self):
        static = self.is_static()
        return {"positionMm": round(self.pos, 1), "at": self.at_name(), "static": static,
                "atAndStatic": self.at_name() if static else None, "moving": self.moving,
                "e1": self.sim.scene.e1,
                "robotsInZone": [k for k, r in self.sim.scene.robots.items() if r["inZone"]]}

    # ---- 报文

    def env(self, body, status, code, message, event_id=None, params=None, data=None, slide_extra=None):
        """NUC 文档 5.1 响应体。字段顺序照文档；滑轨移动的结果多 DetailMessage、Duration 两个字段（7.3.1 示例）。"""
        b = body if isinstance(body, dict) else {}
        d = {"ExecuteStatus": status, "ResultCode": code, "Message": message}
        if slide_extra is not None:
            d.update(slide_extra)
        cmd = b.get("Command")
        d.update({"RequestId": b.get("RequestId"),
                  "EventId": event_id or (b.get("EventId") if is_uuid(b.get("EventId")) else ZERO_UUID),
                  "DeviceId": b.get("DeviceId") if isinstance(b.get("DeviceId"), str) else self.cfg["deviceId"],
                  "Command": cmd.strip() if isinstance(cmd, str) else cmd,
                  "Parameters": params if params is not None else {},
                  "Data": data})
        return d

    def validate(self, body):
        if not isinstance(body, dict):
            return "请求体应为 JSON 对象（NUC 文档 4.1）"
        csa = body.get("CommandSyncAsync")
        if isinstance(csa, bool) or not isinstance(csa, int) or csa not in (0, 1):
            return "CommandSyncAsync 必填，整数 0（异步）或 1（同步）"
        if not is_uuid(body.get("RequestId")):
            return "RequestId 必填，且应为 UUID"
        if body.get("EventId") not in (None, "") and not is_uuid(body.get("EventId")):
            return "EventId 应为 UUID"
        if not isinstance(body.get("DeviceId"), str) or not body["DeviceId"]:
            return "DeviceId 必填，字符串（NUC 驱动登录时输入的 ID）"
        if not isinstance(body.get("Command"), str) or not body["Command"].strip():
            return "Command 必填，字符串"
        if body.get("Parameters") is not None and not isinstance(body["Parameters"], dict):
            return "Parameters 应为 JSON 对象"
        return None

    def callback_base(self, client_ip):
        fixed = self.sim.settings.get("callbackBaseUrl") or ""
        if fixed:
            return fixed.rstrip("/")
        host = f"[{client_ip}]" if ":" in client_ip else client_ip
        return f"http://{host}:{self.cfg['callbackPort']}"

    # ---- 指令入口

    async def handle(self, body, client_ip, ctx):
        problem = self.validate(body)
        if problem:
            return self.env(body, EXEC_ERROR, RC_PARAM, "参数错误：" + problem)
        if body["Command"] != body["Command"].strip():
            ctx["hints"].append("Command 前后有空格（NUC 文档 7.1.1 示例的 \" <CommandName>\" 是占位写法），"
                                "仿真去掉空格后执行，真机可能不认")
        if body["RequestId"] in self.seen_ids:
            ctx["hints"].append("RequestId 重复：NUC 文档要求 RequestId 全局唯一，每个请求新生成一个")
        self.seen_ids.add(body["RequestId"])
        if body["DeviceId"] != self.cfg["deviceId"]:
            return self.env(body, EXEC_ERROR, RC_DEVICE, f"DeviceId 不匹配：本设备是 {self.cfg['deviceId']}")
        cmd = body["Command"].strip()
        if cmd not in self.COMMANDS:
            return self.env(body, EXEC_ERROR, RC_COMMAND, f"不支持的指令：{cmd}（支持：{'、'.join(self.COMMANDS)}）")
        sync = body["CommandSyncAsync"] == 1
        base = self.callback_base(client_ip)
        ctx["sync"], ctx["cmd"] = sync, cmd
        return await getattr(self, "cmd_" + cmd)(body, sync, base, ctx)

    def reject(self, body, message):
        return self.env(body, EXEC_REJECT, RC_REJECT, message)

    def not_idle(self, body):
        tail = {"Stop": "，请先 Reset 上线", "InError": "，请先用 Retry / Ignore 处理错误",
                "Paused": "，请先 Continue", "Resetting": "，请等 Reset 完成"}.get(self.state, "")
        return self.reject(body, f"仅 Idle 状态允许下发设备控制指令（NUC 文档 7.1），当前 {self.state}{tail}")

    def immediate(self, body, sync, base, ctx, final):
        """立即完成的指令：同步直接返回结果；异步先回“已受理”，结果走回调（NUC 文档第 6 章双阶段响应）。"""
        ctx["final"] = (final, "同步返回" if sync else "回调")
        if sync:
            return final
        self.send_callback(base, final)
        return self.env(body, EXEC_SUCCESS, RC_OK, "成功")

    async def wait_or_ack(self, body, task):
        if task["sync"]:
            return await task["future"]  # 同步：执行完一次性返回最终结果
        return self.env(body, EXEC_SUCCESS, RC_OK, "成功")  # 异步第一阶段：已受理

    def new_task(self, body, cmd, sync, base, target_index):
        target = float(self.positions[int(target_index)]["mm"])
        task = {"cmd": cmd, "body": body, "sync": sync, "base": base, "targetIndex": int(target_index),
                "targetMm": target, "startTs": time.time(), "fault": None, "faultAtMm": None, "stallLeft": 0.0,
                "future": asyncio.get_running_loop().create_future() if sync else None}
        if cmd != "Reset" and self.fault:
            task["fault"], self.fault = self.fault, None
            task["faultAtMm"] = (self.pos + target) / 2
            if task["fault"] == "offset":
                off = float(self.cfg.get("faultOffsetMm", 25))
                task["targetMm"] = target - off if target >= self.pos else target + off
            elif task["fault"] == "timeout":
                task["stallLeft"] = float(self.cfg.get("faultTimeoutSeconds", 10))
        self.motion += 1
        task["motion"] = self.motion
        self.task = task
        if abs(self.pos - task["targetMm"]) > 1e-6:
            self.sim.scene.on_motion_start(cmd)
        return task

    async def cmd_GetState(self, body, sync, base, ctx):
        return self.immediate(body, sync, base, ctx, self.env(body, EXEC_SUCCESS, RC_OK, "成功", data=self.state))

    async def cmd_Reset(self, body, sync, base, ctx):
        if self.state == "Resetting":
            return self.reject(body, "设备正在 Resetting，不能重复 Reset")
        if self.task:
            self.end_task(self.task, EXEC_ABORTED, RC_ABORTED, "指令被 Reset 打断")
        self.error, self.paused_from = None, None
        self.state = "Resetting"
        self.reset_left = float(self.cfg.get("resetSeconds", 3))
        task = self.new_task(body, "Reset", sync, base, self.cfg["homeIndex"])
        return await self.wait_or_ack(body, task)

    async def cmd_Close(self, body, sync, base, ctx):
        if self.state in ("Busy", "Resetting") or (self.state == "Paused" and self.paused_from == "Busy"):
            return self.reject(body, f"设备有执行中的指令（{self.state}），不能下线；先 Abort 或等指令完成")
        if self.task:
            self.end_task(self.task, EXEC_ABORTED, RC_ABORTED, "设备下线，指令终止")
        self.state, self.error, self.paused_from = "Stop", None, None
        return self.immediate(body, sync, base, ctx, self.env(body, EXEC_SUCCESS, RC_OK, "成功"))

    async def cmd_Pause(self, body, sync, base, ctx):
        if self.state not in ("Busy", "Idle"):
            return self.reject(body, f"只有 Busy 或 Idle 时能暂停，当前 {self.state}")
        self.paused_from, self.state = self.state, "Paused"
        self.moving = False
        return self.immediate(body, sync, base, ctx, self.env(body, EXEC_SUCCESS, RC_OK, "成功"))

    async def cmd_Continue(self, body, sync, base, ctx):
        if self.state != "Paused":
            return self.reject(body, f"只有 Paused 时能继续，当前 {self.state}")
        self.state, self.paused_from = self.paused_from or "Idle", None
        return self.immediate(body, sync, base, ctx, self.env(body, EXEC_SUCCESS, RC_OK, "成功"))

    async def cmd_Abort(self, body, sync, base, ctx):
        if not (self.state == "Busy" or (self.state == "Paused" and self.paused_from == "Busy")):
            return self.reject(body, f"没有执行中的指令可终止，当前 {self.state}")
        self.end_task(self.task, EXEC_ABORTED, RC_ABORTED, "指令已终止（Abort）")
        self.state, self.paused_from = "Idle", None
        return self.immediate(body, sync, base, ctx, self.env(body, EXEC_SUCCESS, RC_OK, "成功"))

    def check_error_event(self, body):
        if self.state != "InError" or not self.error:
            return self.reject(body, f"没有待处理的错误（当前 {self.state}），Retry / Ignore 只在 InError 时可用")
        eid = body.get("EventId")
        if not eid:
            return self.env(body, EXEC_ERROR, RC_PARAM,
                            "参数错误：Retry / Ignore 必须带失败结果里的 EventId（NUC 文档 7.2.6 / 7.2.7）")
        if eid.lower() != self.error["eventId"].lower():
            return self.env(body, EXEC_ERROR, RC_EVENT, "EventId 与当前待处理的错误不匹配（用失败回调 / 失败响应里的 EventId）")
        cmd = body["Command"].strip()
        if cmd not in self.error["handle"]:
            return self.reject(body, f"这个错误不支持 {cmd}（失败结果里 HandleCommands 为 {self.error['handle']}），请用 Reset 重新上线")
        return None

    async def cmd_Retry(self, body, sync, base, ctx):
        bad = self.check_error_event(body)
        if bad:
            return bad
        failed = self.error["task"]
        self.error = None
        self.state = "Busy"
        task = self.new_task(body, "Retry", sync, base, failed["targetIndex"])
        return await self.wait_or_ack(body, task)

    async def cmd_Ignore(self, body, sync, base, ctx):
        bad = self.check_error_event(body)
        if bad:
            return bad
        self.error, self.state = None, "Idle"
        return self.immediate(body, sync, base, ctx, self.env(body, EXEC_SUCCESS, RC_OK, "成功"))

    async def cmd_SlideByCursorIndex(self, body, sync, base, ctx):
        if self.state != "Idle":
            return self.not_idle(body)
        idx = (body.get("Parameters") or {}).get("CursorIndex")
        if isinstance(idx, bool) or not isinstance(idx, int):
            return self.env(body, EXEC_ERROR, RC_PARAM, "参数错误：Parameters.CursorIndex 必填，整数（NUC 文档 7.3.1）")
        if idx not in self.positions:
            have = "、".join(f"{i}={p['name']}" for i, p in sorted(self.positions.items()))
            return self.env(body, EXEC_ERROR, RC_PARAM, f"参数错误：CursorIndex={idx} 没有点位（可用：{have}）")
        self.state = "Busy"
        task = self.new_task(body, "SlideByCursorIndex", sync, base, idx)
        return await self.wait_or_ack(body, task)

    # ---- 指令结束：同步交给等待中的请求，异步发回调

    def end_task(self, task, status, code, message):
        if task is None:
            return
        if task is self.task:
            self.task = None
        self.moving = False
        body = task["body"]
        event_id, params = None, None
        if status in (EXEC_ERROR, EXEC_TIMEOUT, EXEC_ABORTED):
            # NUC 文档 7.1.3：走 OnError 的结果用 EventId 标识这次异常；Error / Timeout 的重试 / 忽略要带回来
            event_id = body.get("EventId") if is_uuid(body.get("EventId")) else str(uuid.uuid4())
        if status in (EXEC_ERROR, EXEC_TIMEOUT):
            handle = [] if task["fault"] in ("lostcontrol", "estop") else ["Retry", "Ignore"]
            params = {"HandleCommands": handle}
            self.error = {"eventId": event_id, "task": task, "handle": handle}
            self.state = "InError"
        extra = None
        if task["cmd"] in ("SlideByCursorIndex", "Retry"):
            extra = {"DetailMessage": None, "Duration": int((time.time() - task["startTs"]) * 1000)}
        final = self.env(body, status, code, message, event_id=event_id, params=params, slide_extra=extra)
        self.record_result(final, "同步返回" if task["sync"] else "回调", task)
        if status == EXEC_SUCCESS and self.is_static():
            self.report_static()  # 本来就在目标点位、没动：立刻记到位静止（动过的话等停稳后在 tick 里记）
        if task["sync"]:
            if not task["future"].done():
                task["future"].set_result(final)
        else:
            self.send_callback(task["base"], final, {"lostcontrol": 1, "estop": 2}.get(task["fault"], 0))

    def record_result(self, final, via, task=None):
        ev = {"cmd": final["Command"], "requestId": final["RequestId"], "via": via, "resp": final,
              "motion": task["motion"] if task else self.motion}
        if task:
            ev["target"] = task["targetIndex"]
            ev["targetName"] = self.positions[task["targetIndex"]]["name"]
            ev["positionMm"] = round(self.pos, 1)
        self.last = {"cmd": final["Command"], "status": final["ExecuteStatus"], "code": final["ResultCode"],
                     "message": final["Message"], "duration": final.get("Duration"), "via": via,
                     "target": ev.get("targetName"), "time": fmt_ms(time.time())}
        self.sim.log.note("rail_result", f"{final['Command']} 最终结果 ExecuteStatus={final['ExecuteStatus']} "
                                         f"{final['ResultCode']} {final['Message']}（{via}）", **ev)

    def report_static(self):
        """仿真相机看到交互口停稳：记一条物理事件，标上是第几次移动之后停稳的。"""
        name = self.at_name()
        where = f"在 {name} 到位并静止" if name else "停在两个点位之间（不在 IN/OUT）"
        self.sim.log.note("physical", f"交互口{where}：{self.pos:.1f} mm（仿真相机）",
                          position=name, positionMm=round(self.pos, 1), motion=self.motion)
        self.sensor_reported = None

    def send_callback(self, base, final, affect=0):
        if final["ExecuteStatus"] == EXEC_SUCCESS:
            url, payload = base + "/api/OnResponse", final
        else:
            url = base + "/api/OnError"
            payload = {"ErrorType": 1 if final["ExecuteStatus"] == EXEC_ABORTED else 0, "DeviceErrorAffect": affect,
                       "EventDeviceId": self.cfg["deviceId"]}
            payload.update(final)
        self.sim.post_json_later("callback", url, payload, f"{final['Command']} 结果回调")

    # ---- 时间推进

    def move_toward(self, goal, step):
        d = goal - self.pos
        if abs(d) <= 1e-9:
            self.moving = False
            return True
        self.pos = goal if abs(d) <= step else self.pos + (step if d > 0 else -step)
        self.moving = True
        self.settle_left = float(self.cfg.get("settleMs", 500))
        return False

    def tick(self, now, dt):
        speed = float(self.cfg.get("speedMmPerSecond", 200)) * dt
        task = self.task
        if self.state == "Resetting" and task:
            self.reset_left -= dt
            arrived = self.move_toward(task["targetMm"], speed)
            if arrived and self.reset_left <= 0:
                self.state = "Idle"
                self.end_task(task, EXEC_SUCCESS, RC_OK, "成功")
        elif self.state == "Busy" and task:
            fault, at = task["fault"], task["faultAtMm"]
            if fault in ("error", "timeout", "lostcontrol", "estop") and abs(self.pos - at) <= 1e-6:
                self.moving = False
                if fault == "error":
                    self.end_task(task, EXEC_ERROR, RC_MOTION, "轨道运动异常（仿真注入）")
                elif fault == "lostcontrol":
                    self.end_task(task, EXEC_ERROR, RC_LOST, "设备失控（仿真注入）")
                elif fault == "estop":
                    self.end_task(task, EXEC_ERROR, RC_ESTOP, "急停（仿真注入）")
                else:
                    task["stallLeft"] -= dt
                    if task["stallLeft"] <= 0:
                        self.end_task(task, EXEC_TIMEOUT, RC_TIMEOUT, "运动超时（仿真注入）")
            else:
                goal = at if fault in ("error", "timeout", "lostcontrol", "estop") else task["targetMm"]
                if self.move_toward(goal, speed) and goal == task["targetMm"]:
                    self.state = "Idle"
                    self.end_task(task, EXEC_SUCCESS, RC_OK, "Succ")
        else:
            self.moving = False
        if not self.moving and self.settle_left > 0:
            self.settle_left -= dt * 1000
        static = self.is_static()
        if static and not self.was_static:
            self.report_static()
        self.was_static = static


# ---------------------------------------------------------------- 物理场景（机器人、物件）

class Scene:
    """仿真的物理世界：E1 上有没有物件、机器人 A/B 在不在设备运动区。真机上靠相机和机器人自己判断，接口里没有这些。"""

    def __init__(self, sim):
        self.sim = sim
        self.reset()

    def reset(self):
        cfg = self.sim.cfg["scene"]
        self.robots = {r: {"inZone": False, "holding": None} for r in cfg["robots"]}
        self.objects = list(cfg["objects"])
        self.e1 = None              # 交互口托盘（交互位 E1）上的物件
        self.platform = {}          # 浦江流程：耗材 uuid -> 物件（物件在平台里）
        self.stuck = []             # 浦江流程：卸载时交互口没在内侧，物件没地方放
        self.loaded_count = 0
        self.taken_out = []
        self.violations = []

    def free_object(self):
        used = {self.e1} | set(self.platform.values()) | set(self.stuck) | \
               {r["holding"] for r in self.robots.values()} | set(self.taken_out)
        return next((o for o in self.objects if o not in used), None)

    def violation(self, rule, detail):
        v = {"time": fmt_ms(time.time()), "rule": rule, "detail": detail}
        self.violations.append(v)
        self.sim.log.note("violation", f"{rule}：{detail}", rule=rule)

    def on_motion_start(self, cmd):
        if self.e1 and self.sim.settings["mode"] == "competition":
            self.violation("交互口空载铁律", f"交互口带着 {self.e1} 运动（{cmd}）；比赛要求 OUT/IN 运动时必须空载")
        inside = [k for k, r in self.robots.items() if r["inZone"]]
        if inside:
            self.violation("设备运动区联锁", f"交互口运动（{cmd}）时机器人 {'、'.join(inside)} 还在设备运动区内")

    def at_in_static(self):
        rail = self.sim.rail
        return rail.is_static() and rail.at_name() == "IN"

    def on_labware_loaded(self, created):
        """浦江流程：滑轨把物件送到内侧后调“加载耗材”，平台把物件从交互口内侧取进去。比赛规则下只是登记，不碰物件。"""
        if self.sim.settings["mode"] != "pujiang":
            return
        for lw in created:
            if self.e1 and self.at_in_static():
                obj, self.e1 = self.e1, None
                self.platform[lw["uuid"]] = obj
                self.loaded_count += 1
                self.sim.log.note("physical", f"平台从交互口内侧取走 {obj}（登记为 {lw['labwareName']}）")
            else:
                self.sim.log.note("physical", f"提示：加载 {lw['labwareName']} 时交互口内侧没有物件"
                                              f"（浦江流程应先放耗材、滑轨收回到内部再加载）", warn=True)

    def on_labware_unloaded(self, removed):
        if self.sim.settings["mode"] != "pujiang":
            return
        for lw in removed:
            obj = self.platform.pop(lw["uuid"], None)
            if obj is None:
                continue
            if self.e1 is None and self.at_in_static():
                self.e1 = obj
                self.sim.log.note("physical", f"平台把 {obj} 放到交互口内侧（卸载 {lw['labwareName']}）")
            else:
                self.stuck.append(obj)
                self.sim.log.note("physical", f"提示：卸载 {lw['labwareName']} 时交互口不在内侧到位静止或上面已有物件，"
                                              f"{obj} 没地方放（浦江流程应先把滑轨移到内部再卸载）", warn=True)

    def robot_action(self, robot, action, obj=None):
        if robot not in self.robots:
            raise SimError(f"没有机器人 {robot}（可选：{'、'.join(self.robots)}）")
        r, rail = self.robots[robot], self.sim.rail
        if action == "enter":
            if rail.moving:
                self.violation("设备运动区联锁", f"交互口运动时机器人 {robot} 进入设备运动区")
            r["inZone"] = True
            msg = f"机器人 {robot} 进入设备运动区"
        elif action == "exit":
            r["inZone"] = False
            msg = f"机器人 {robot} 退出设备运动区"
        elif action in ("place", "take"):
            if not r["inZone"]:
                raise SimError(f"机器人 {robot} 不在设备运动区，够不到交互位 E1（先 enter）")
            if not (rail.is_static() and rail.at_name() == "OUT"):
                raise SimError("交互口没有在 OUT 弹出到位并静止，不能放 / 取")
            if action == "place":
                if self.e1:
                    raise SimError(f"E1 上已经有 {self.e1}")
                obj = obj or r["holding"] or self.free_object()
                if obj is None:
                    raise SimError("没有可放的物件")
                if obj in self.taken_out:
                    self.taken_out.remove(obj)
                self.e1, r["holding"] = obj, None
                msg = f"机器人 {robot} 把 {obj} 放到 E1 并松开夹爪"
            else:
                if not self.e1:
                    raise SimError("E1 上没有物件")
                if r["holding"]:
                    raise SimError(f"机器人 {robot} 手上已经有 {r['holding']}")
                obj, self.e1 = self.e1, None
                r["holding"] = obj
                if self.sim.settings["mode"] == "pujiang":
                    self.taken_out.append(obj)
                msg = f"机器人 {robot} 从 E1 取回 {obj}"
        elif action == "deliver":
            if not r["holding"]:
                raise SimError(f"机器人 {robot} 手上没有物件")
            obj, r["holding"] = r["holding"], None
            msg = f"机器人 {robot} 把 {obj} 送到终点放置位（R1/R2）并松开"
        else:
            raise SimError("action 只能是 enter / exit / place / take / deliver")
        self.sim.log.note("sim", msg, robot=robot, action=action, obj=obj)
        return msg


# ---------------------------------------------------------------- 检查点

T1_RULES = [
    ("T1-B-13", "确认正在运行", "本轮 run 请求后 GET /workcellstate 返回 Running，run.name 为 MEGA_STEP_TEST；请求与响应属于同一 Platform 及本轮时间段。"),
    ("T1-B-14", "提交暂停", "本轮已确认 Running 后提交 POST /pause，收到本次业务成功响应；评价请求受理，不臆设 Paused 状态字段。"),
    ("T1-B-15", "提交继续", "本轮 pause 成功之后提交 POST /continue，收到本次业务成功响应。"),
    ("T1-B-16", "提交终止", "本轮 continue 成功之后提交 POST /stop，收到本次业务成功响应。"),
    ("T1-B-17", "确认停止", "本轮已观测到 Running 且 stop 成功后，再次 GET /workcellstate 返回 NotRunning；启动前的 NotRunning 不计本项目，主动终止不计实验正常完成。"),
    ("T1-B-18", "卸载测试耗材", "DELETE /lhdlabwares 请求体为本轮 labwareName 组成的数组，响应包含对应 uuid。"),
    ("T1-B-19", "确认卸载结果", "再次 GET /labwares，本轮已卸载 uuid 不再存在。"),
    ("T1-B-20", "下线设备", "本轮已确认 NotRunning 并卸载测试耗材后，POST /offlinedevices 收到本次业务成功响应。"),
]


def compute_checks(sim):
    """按检查点原文判本轮的调用记录。只看本轮、限时内、Platform 正确、业务成功（code=成功码）的调用。"""
    cfg = sim.cfg
    t0 = sim.log.round_start
    limit = cfg["task1"]["timeLimitMinutes"] * 60
    platform = cfg["labillion"]["platform"]
    run_name = cfg["task1"]["runName"]
    evs = sim.log.events
    lab = [e for e in evs if e["kind"] == "labillion" and "status" in e]

    def good(e, key):
        return (e["key"] == key and e.get("ok") and e["ts"] - t0 <= limit
                and (e.get("headers") or {}).get("Platform") == platform)

    def first(key, after=0, pred=None):
        return next((e for e in lab if e["seq"] > after and good(e, key) and (pred is None or pred(e))), None)

    def data(e):
        r = e.get("resp") if e else None
        return r.get("data") if isinstance(r, dict) else None

    def state_is(value):
        return lambda e: isinstance(data(e), dict) and data(e).get("runState") == value

    def running_named(e):
        d = data(e)
        return (isinstance(d, dict) and d.get("runState") == "Running" and isinstance(d.get("run"), dict)
                and d["run"].get("name") == run_name)

    def ref(e):
        return {"seq": e["seq"], "time": e["time"][11:23]} if e else None

    # ---- 任务1 官方明细（T1-B-13 ~ 20）
    # 本轮可能成功 run 了不止一次：每次 run 到下一次 run 之间单独判一条链，取得分最多的那条。
    # T1-B-13 的 Running 要在这次 run 之后、第一次 stop 之前查到（stop 后“终止中”仍返回 Running，那不算确认正在运行）。
    runs = [e for e in lab if good(e, "run")]

    def chain(i):
        r = runs[i]
        end = runs[i + 1]["seq"] if i + 1 < len(runs) else float("inf")

        def nxt(key, after, pred=None):
            e = first(key, after, pred)
            return e if e and e["seq"] < end else None
        stop1 = nxt("stop", r["seq"])

        def before_stop(pred):
            return lambda e: pred(e) and (stop1 is None or e["seq"] < stop1["seq"])
        c = {"run": r, "running": nxt("workcellstate", r["seq"], before_stop(state_is("Running"))),
             "e13": nxt("workcellstate", r["seq"], before_stop(running_named))}
        c["e14"] = nxt("pause", c["running"]["seq"]) if c["running"] else None
        c["e15"] = nxt("continue", c["e14"]["seq"]) if c["e14"] else None
        c["e16"] = nxt("stop", c["e15"]["seq"]) if c["e15"] else None
        c["e_stop"] = nxt("stop", c["running"]["seq"]) if c["running"] else None
        c["e17"] = nxt("workcellstate", c["e_stop"]["seq"], state_is("NotRunning")) if c["e_stop"] else None
        return c

    chains = [chain(i) for i in range(len(runs))]
    best = max(chains, key=lambda c: sum(1 for k in ("e13", "e14", "e15", "e16", "e17") if c[k]), default={})
    e_run, e_running = best.get("run"), best.get("running")
    e13, e14, e15, e16, e_stop, e17 = (best.get(k) for k in ("e13", "e14", "e15", "e16", "e_stop", "e17"))
    if e13:
        h13 = ""
    elif not e_run:
        h13 = "还没有成功的 POST /run"
    elif e_running:
        h13 = (f"查到了 Running，但 run.name 是 {data(e_running)['run'].get('name')}，要求 {run_name}"
               f"（运行时传 runName）")
    else:
        h13 = "run 之后、stop 之前还没查到 Running"

    # T1-B-18 逐次判卸载请求：请求体里每个名字都是本轮在这次卸载之前加载的，回复里有对应的 uuid。满足一次就算，后面的请求不撤销
    def loaded_before(seq):
        names = {}
        for e in lab:
            if e["seq"] >= seq:
                break
            if good(e, "load") and isinstance(data(e), list):
                names.update({lw.get("labwareName"): lw.get("uuid") for lw in data(e) if isinstance(lw, dict)})
        return names
    e18, gone, foreign = None, set(), []
    for e in lab:
        if not good(e, "unload"):
            continue
        names = e.get("req") if isinstance(e.get("req"), list) else []
        uuids = data(e) if isinstance(data(e), list) else []
        before = loaded_before(e["seq"])
        bad = [n for n in names if n not in before]
        if names and not bad and all(before[n] in uuids for n in names):
            e18, gone = e, {before[n] for n in names}
            break
        foreign += [n for n in bad if n not in foreign]
    if e18:
        h18 = ""
    elif foreign:
        h18 = f"卸载请求里有不是本轮加载的耗材：{'、'.join(foreign)}（请求体只能放本轮加载过的 labwareName）"
    else:
        h18 = "还没有成功卸载本轮加载的耗材" if loaded_before(float("inf")) else "本轮还没有加载过测试耗材"
    e19 = first("labwares", e18["seq"], lambda e: isinstance(data(e), list) and not (
        gone & {lw.get("uuid") for lw in data(e) if isinstance(lw, dict)})) if e18 else None
    e20 = first("offline", max(e17["seq"], e18["seq"])) if (e17 and e18) else None

    hints = {"T1-B-13": h13,
             "T1-B-14": "" if e14 else ("run 之后先查到 Running" if not e_running else "查到 Running 之后还没有成功的 /pause"),
             "T1-B-15": "" if e15 else ("先完成暂停" if not e14 else "暂停之后还没有成功的 /continue"),
             "T1-B-16": "" if e16 else ("先完成继续" if not e15 else "继续之后还没有成功的 /stop"),
             "T1-B-17": "" if e17 else ("先观测到 Running 并 stop 成功" if not e_stop else "stop 之后还没查到 NotRunning"),
             "T1-B-18": h18,
             "T1-B-19": "" if e19 else ("先完成卸载" if not e18 else "卸载之后还没有不含已卸载 uuid 的 /labwares"),
             "T1-B-20": "" if e20 else "要在确认 NotRunning（T1-B-17）并卸载测试耗材（T1-B-18）之后再 /offlinedevices"}
    passed = {"T1-B-13": e13, "T1-B-14": e14, "T1-B-15": e15, "T1-B-16": e16, "T1-B-17": e17,
              "T1-B-18": e18, "T1-B-19": e19, "T1-B-20": e20}
    official = [{"id": i, "name": n, "rule": rule, "pass": bool(passed[i]), "evidence": ref(passed[i]),
                 "hint": hints[i]} for i, n, rule in T1_RULES]

    # ---- 任务1 前 12 项：官方明细“演示略”，按“基础登录、设备上线、耗材登记等”推测，只作参考
    e_login, e_on, e_load = first("login"), first("online"), first("load")
    first_loaded = {lw.get("uuid") for lw in (data(e_load) or []) if isinstance(lw, dict)}

    def all_idle(e):
        d = data(e)
        return isinstance(d, list) and d and all(x.get("deviceState") == "Idle" for x in d)
    pre_items = [
        ("登录", e_login),
        ("查询设备", first("devices")),
        ("上线设备", e_on),
        ("确认设备都是 Idle", first("devicestates", e_on["seq"], all_idle) if e_on else None),
        ("查询耗材类型", first("labwaretypes")),
        ("加载耗材", e_load),
        ("确认耗材已登记", first("labwares", e_load["seq"], lambda e: first_loaded <= {
            lw.get("uuid") for lw in (data(e) or []) if isinstance(lw, dict)}) if e_load else None),
        ("查询流程", first("processdesigns")),
        ("预览", first("preview")),
        ("启动前确认 NotRunning", first("workcellstate", 0, lambda e: state_is("NotRunning")(e) and (
            not runs or e["seq"] < runs[0]["seq"]))),
        (f"运行（runName={run_name}）", first("run", 0, lambda e: isinstance(e.get("req"), dict)
                                              and e["req"].get("runName") == run_name)),
        ("查询错误 / 报警", first("errors") or first("warnings")),
    ]
    pre = [{"id": f"推测-{i:02d}", "name": n, "pass": bool(e), "evidence": ref(e)} for i, (n, e) in enumerate(pre_items, 1)]

    # ---- 任务3 软件智能体：Labillion 接口阶段 B-L01 ~ 07
    def stage(sid, name, keys, done):
        """keys：算时间段用的接口；done：这个阶段按顺序做完了的那条证据（没有就是 None）。"""
        hits = [e for e in lab if any(good(e, k) for k in keys)]
        window = None
        if hits:
            window = [hits[0]["time"][11:23], fmt_ms(max(e.get("tsResp", e["ts"]) for e in hits))[11:23]]
        return {"id": sid, "name": name, "pass": bool(done), "window": window, "calls": len(hits)}
    e_on_after = first("online", e_login["seq"]) if e_login else None
    load_seen = first("labwares", e_load["seq"], lambda e: first_loaded <= {
        lw.get("uuid") for lw in (data(e) or []) if isinstance(lw, dict)}) if e_load else None
    e_pause = first("pause")
    e_cont = first("continue", e_pause["seq"]) if e_pause else None
    bl = [stage("B-L01", "登录上线", ["login", "online"], e_on_after),
          stage("B-L02", "耗材", ["load"], load_seen),
          stage("B-L03", "预览", ["preview"], first("preview")),
          stage("B-L04", "运行", ["run"], e_running),
          stage("B-L05", "暂停继续", ["pause", "continue"], e_cont),
          stage("B-L06", "终止", ["stop"], e17),
          stage("B-L07", "卸载下线", ["unload", "offline"], first("offline", e18["seq"]) if e18 else None)]

    # ---- 任务3 软件智能体：设备口控制 B-C01 / B-C02（指令成功 + 没违反联锁 + 实际到位静止 + 智能体用相机核验）
    req_seq = {e["req"]["RequestId"]: e["seq"] for e in evs if e["kind"] == "rail" and isinstance(e.get("req"), dict)
               and isinstance(e["req"].get("RequestId"), str)}

    def bc(sid, name, pos_name):
        idx = sim.rail.index_of(pos_name)
        done = [e for e in evs if e["kind"] == "rail_result" and e.get("target") == idx
                and e.get("cmd") in ("SlideByCursorIndex", "Retry") and e["resp"]["ExecuteStatus"] == EXEC_SUCCESS]
        count, last, note = 0, None, ""
        for d in done:
            k = d.get("motion")  # 这次移动的编号：到位证据和核验都必须是这一次移动之后、下一次移动之前的
            start = req_seq.get(d.get("requestId"), d["seq"])
            broke = next((v for v in evs if v["kind"] == "violation" and start <= v["seq"] <= d["seq"]), None)
            if broke:
                note = f"#{broke['seq']} 这次运动违反联锁，判无效：{broke['summary']}"
                continue
            phys = next((p for p in evs if p["kind"] == "physical" and p.get("motion") == k and "positionMm" in p), None)
            if not phys or phys.get("position") != pos_name:
                note = ("接口报成功，但交互口实际没停在点位上（仿真相机）" if phys else
                        "交互口还没停稳就发了下一条移动指令，没有到位静止的证据")
                continue
            # 核验只认相机（仿真里是 GET /sim/port）独立看到到位静止；GetState 只是接口返回，不算物理证据
            check = next((x for x in evs if x["seq"] > phys["seq"] and x.get("motion") == k
                          and x["kind"] == "sensor" and x.get("position") == pos_name), None)
            if not check:
                note = "到位了，但下一次移动前智能体没有用相机核验到位静止（仿真里是 GET /sim/port；GetState 不算）"
                continue
            count += 1
            last = {"command": ref(d), "physical": ref(phys), "verify": ref(check)}
            note = ""
        return {"id": sid, "name": name, "pass": count > 0, "count": count, "last": last, "hint": note}
    bcs = [bc("B-C01", "交互口弹出 OUT + 到位核验", "OUT"), bc("B-C02", "交互口收回 IN + 到位核验", "IN")]

    # ---- 浦江调用流程（上料 → 运行 → 数据 → 下料）
    sc, r = sim.scene, sim.lab.run
    pj = {"mode": sim.settings["mode"], "loaded": sc.loaded_count, "inPlatform": list(sc.platform.values()),
          "takenOut": list(sc.taken_out), "stuck": list(sc.stuck),
          "run": (r["status"] if r else None), "dataPush": (r["dataPush"] if r else None),
          "finished": bool(r and r["status"] == "completed" and sc.loaded_count and not sc.platform
                           and not sc.e1 and len(sc.taken_out) >= sc.loaded_count)}

    return {"official": official, "pre": pre, "bl": bl, "bc": bcs, "pujiang": pj,
            "score": {"official": sum(x["pass"] for x in official), "officialTotal": len(official),
                      "pre": sum(x["pass"] for x in pre), "preTotal": len(pre)}}


# ---------------------------------------------------------------- 示例智能体（页面上的“运行示例智能体”按钮）

DEMO_NAMES = {"all": "全部（浦江流程 → 任务1 → 交互口 E1）", "t1": "任务1 设备接入", "e1": "交互口 E1",
              "flow": "浦江调用流程"}


class DemoRunner:
    """在后台跑 demo_agent.py（另一个进程，和外部智能体一样走网络接口），把它的输出留给页面看。一次只跑一个。"""

    def __init__(self, sim):
        self.sim = sim
        self.proc = None
        self.lines = []
        self.scenario = None
        self.rc = None
        self.reported = True
        self.echo = False  # --demo 时把输出也打印到终端

    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, scenario, faults=False, use_async=False):
        if self.running():
            raise SimError("示例智能体正在跑，等它跑完，或先点「停止示例」")
        if scenario not in DEMO_NAMES:
            raise SimError(f"没有这个场景：{scenario}")
        cmd = [sys.executable, "-u", str(HERE / "demo_agent.py"), scenario, "--config", self.sim.cfg_path]
        if faults:
            cmd.append("--faults")
        if use_async:
            cmd.append("--async")
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(HERE),
                                     env=dict(os.environ, PYTHONIOENCODING="utf-8"))
        self.lines, self.rc, self.scenario, self.reported, self.stopped = [], None, scenario, False, False
        # 示例智能体跑浦江流程时会临时调倍速和推送地址；不管怎么结束，都还原成开始前的样子
        self.saved = (self.sim.speed, self.sim.settings["dataPushUrl"])
        threading.Thread(target=self.pump, args=(self.proc,), daemon=True).start()
        extra = "，演示故障处理" if faults else ""
        extra += "，交互口异步回调" if use_async else ""
        self.sim.log.note("sim", f"开始运行示例智能体：{DEMO_NAMES[scenario]}{extra}")
        return f"示例智能体开始跑：{DEMO_NAMES[scenario]}"

    def pump(self, proc):
        for raw in proc.stdout:
            line = raw.decode("utf-8", "replace").rstrip()
            self.lines.append(line)
            if len(self.lines) > 800:
                del self.lines[:200]
            if self.echo:
                print(line, flush=True)
        self.rc = proc.wait()

    def stop(self):
        if not self.running():
            raise SimError("示例智能体没在跑")
        self.stopped = True
        self.proc.terminate()
        return "已停止示例智能体"

    def check_finished(self):
        """仿真循环里调用：示例智能体结束时还原设置，并在调用记录里记一条。"""
        if not self.reported and self.proc is not None and self.proc.poll() is not None and self.rc is not None:
            self.reported = True
            self.sim.speed, self.sim.settings["dataPushUrl"] = self.saved
            result = ("被手动停止" if self.stopped else
                      "全部通过" if self.rc == 0 else "有没通过的（看页面上的示例智能体输出）")
            self.sim.log.note("sim", f"示例智能体结束：{DEMO_NAMES[self.scenario]}，{result}")

    def view(self):
        return {"running": self.running(), "scenario": self.scenario, "stopped": getattr(self, "stopped", False),
                "name": DEMO_NAMES.get(self.scenario, ""), "rc": None if self.running() else self.rc,
                "lines": self.lines[-400:]}


# 页面“手动操作”的按钮 → (方法, 路径)。请求体按当前状态现生成，见 Sim.manual_body
MANUAL_LAB = {
    "login": ("POST", "/login"), "devices": ("GET", "/devices"), "online": ("POST", "/onlinedevices"),
    "devicestates": ("GET", "/devicestates"), "labwaretypes": ("GET", "/labwaretypes"),
    "load": ("POST", "/lhdlabwares"), "labwares": ("GET", "/labwares"), "processdesigns": ("GET", "/processdesigns"),
    "preview": ("POST", "/preview"), "run": ("POST", "/run"), "workcellstate": ("GET", "/workcellstate"),
    "pause": ("POST", "/pause"), "continue": ("POST", "/continue"), "stop": ("POST", "/stop"),
    "unload": ("DELETE", "/lhdlabwares"), "offline": ("POST", "/offlinedevices"),
    "errors": ("GET", "/errors"), "warnings": ("POST", "/warnings"),
    "Retry": ("POST", "/handleerror"), "Ignore": ("POST", "/handleerror"), "Abort": ("POST", "/handleerror"),
}
MANUAL_RAIL = ("Reset", "GetState", "OUT", "IN", "Camera", "Pause", "Continue", "Abort", "Retry", "Ignore", "Close")


# ---------------------------------------------------------------- 仿真总控

class Sim:
    def __init__(self, cfg, speed, quiet, cfg_path):
        self.cfg = cfg
        self.cfg_path = cfg_path
        self.speed = speed
        lab, rail = cfg["labillion"], cfg["rail"]
        self.settings = {"mode": cfg["scene"]["mode"], "successCode": lab["successCode"],
                         "strictHeaders": lab["strictHeaders"], "dataPushUrl": lab.get("dataPushUrl", ""),
                         "callbackBaseUrl": rail.get("callbackBaseUrl", "")}
        self.log = EventLog(HERE / "logs", quiet)
        self.log.new_round()
        self.bg = set()
        self.lab = Labillion(self)
        self.rail = Rail(self)
        self.scene = Scene(self)
        self.demo = DemoRunner(self)
        self.manual_token = None  # 页面“手动操作”登录后拿到的 Token
        self.log.note("sim", f"仿真台启动（{MODE_NAMES[self.settings['mode']]}，{self.speed:g} 倍速）")

    def new_round(self, mode=None):
        if mode in MODE_NAMES:
            self.settings["mode"] = mode
        self.log.new_round()
        self.rail.reset()
        self.lab.reset()
        self.scene.reset()
        self.manual_token = None
        self.log.note("sim", f"重置为统一初始状态，开始新一轮（{MODE_NAMES[self.settings['mode']]}；"
                             f"任务1 限时 {self.cfg['task1']['timeLimitMinutes']} 分钟从现在起算）")

    async def loop(self):
        last = time.time()
        while True:
            await asyncio.sleep(0.05)
            now = time.time()
            dt, last = (now - last) * self.speed, now
            try:
                self.lab.tick(now, dt)
                self.rail.tick(now, dt)
                self.demo.check_finished()
            except Exception:  # 仿真循环不能停
                traceback.print_exc()

    def post_json_later(self, kind, url, payload, label):
        async def go():
            await asyncio.sleep(0.05)  # 让“已受理”的响应先发出去
            t0 = time.time()
            try:
                status, text = await asyncio.to_thread(http_call, "POST", url, payload)
                ok, result = 200 <= status < 300, f"HTTP {status}"
            except Exception as e:
                status, text, ok, result = None, "", False, f"发送失败：{e}（客户端回调服务没开？）"
            self.log.note(kind, f"{label} → {url} {result}", ts=t0, url=url, req=payload, status=status,
                          respText=text[:300], ok=ok)
        task = asyncio.get_running_loop().create_task(go())
        self.bg.add(task)
        task.add_done_callback(self.bg.discard)

    # ---- 页面“手动操作”：替智能体真的调一次接口（走 8080 / 40000 端口），记录和判分跟智能体调的一样

    def manual_body(self, action):
        lab, task1 = self.lab, self.cfg["task1"]
        if action == "login":
            user = lab.cfg["users"][0]
            return {"tenantId": lab.cfg["tenantId"], "userName": user["userName"], "password": user["password"]}
        if action == "load":
            plates = [t["name"] for t in lab.cfg["labwareTypes"] if t["category"] == "Plate"]
            occupied = {lw["location"] for lw in lab.labwares}
            free = [loc for loc in lab.all_locations if loc not in occupied]
            free.sort(key=lambda loc: not loc.startswith("Carousel"))
            if not plates or not free:
                raise SimError("没有可用的耗材类型或空位置")
            return [{"labwareType": plates[0], "label": "manual", "location": free[0]}]
        if action == "preview":
            return {"Data": [{"process": task1["process"], "iterationCount": "1"}]}
        if action == "run":
            return {"runName": task1["runName"], "data": [{"process": task1["process"], "iterationCount": "1"}]}
        if action == "unload":
            names = [lw["labwareName"] for lw in lab.labwares if lw["uuid"] in lab.round_loaded]
            if not names:
                raise SimError("本轮没有加载过、还没卸载的耗材（先点 6「加载耗材」）")
            return names
        if action in ("Retry", "Ignore", "Abort"):
            if not lab.errors:
                raise SimError("现在没有流程错误可处理（先在「场景」里注入流程错误）")
            return {"errorId": lab.errors[0]["uuid"], "handleOption": action}
        return None

    async def manual(self, body):
        if self.demo.running():
            raise SimError("示例智能体正在跑，先等它跑完，或点「停止示例」")
        target, action = body.get("target"), body.get("action")
        srv = self.cfg["server"]
        host = "127.0.0.1" if srv["host"] in ("0.0.0.0", "", "::") else srv["host"]
        headers = {"Content-Type": "application/json", "X-Sim-Manual": "1"}
        if target == "labillion":
            if action not in MANUAL_LAB:
                raise SimError(f"没有这个操作：{action}")
            method, sub = MANUAL_LAB[action]
            payload = self.manual_body(action)
            scheme = "https" if srv.get("labillionCertFile") else "http"
            url = f"{scheme}://{host}:{srv['labillionPort']}{API}{sub}"
            headers.update({"Accept-Language": "zh-CN", "Platform": self.cfg["labillion"]["platform"]})
            if action != "login" and self.manual_token:
                headers["Authorization"] = "Bearer " + self.manual_token
        elif target == "rail":
            if action not in MANUAL_RAIL:
                raise SimError(f"没有这个操作：{action}")
            if action == "Camera":  # 仿真相机：智能体独立核验到位静止（B-C 只认这个）
                return {"request": {"method": "GET", "url": f"http://{host}:{srv['consolePort']}/sim/port", "body": None},
                        "status": 200, "response": self.sensor_query()}
            method, url = "POST", f"http://{host}:{srv['railPort']}{NUC_PATH}"
            payload = {"CommandSyncAsync": 1, "RequestId": str(uuid.uuid4()), "DeviceId": self.cfg["rail"]["deviceId"],
                       "Command": action, "Parameters": {}}
            if action in ("OUT", "IN"):
                payload["Command"] = "SlideByCursorIndex"
                payload["Parameters"] = {"CursorIndex": self.rail.index_of(action)}
            if action in ("Retry", "Ignore"):
                if not self.rail.error:
                    raise SimError("交互口没有待处理的错误（它不在 InError 状态）")
                payload["EventId"] = self.rail.error["eventId"]
        else:
            raise SimError("target 只能是 labillion 或 rail")
        try:
            status, text = await asyncio.to_thread(http_call, method, url, payload, headers, 60.0)
        except OSError as e:
            raise SimError(f"接口没连上：{e}")
        try:
            resp = json.loads(text)
        except ValueError:
            resp = text
        if (target == "labillion" and action == "login" and status == 200 and isinstance(resp, dict)
                and resp.get("code") == self.settings["successCode"]):
            self.manual_token = resp["data"]
        return {"request": {"method": method, "url": url, "body": payload}, "status": status, "response": resp}

    # ---- 两个设备接口的统一入口：记录请求和响应

    async def labillion_dispatch(self, request, key):
        t0 = time.time()
        raw = await request.body()
        client = request.client.host if request.client else "127.0.0.1"
        hdr = request.headers
        auth = hdr.get("authorization")
        ev = self.log.add("labillion", ts=t0, client=client, method=request.method, path=request.url.path, key=key,
                          headers={"Platform": hdr.get("platform"), "Accept-Language": hdr.get("accept-language"),
                                   "Content-Type": hdr.get("content-type"),
                                   "Authorization": (auth[:20] + f"…（共 {len(auth)} 字符）") if auth else None},
                          req=decode_for_log(raw))
        try:
            status, payload, hints = self.lab.handle(key, request.method, hdr, raw, client)
        except Exception as e:  # 仿真自己的 bug：按文档格式回 500，记录照常落盘
            traceback.print_exc()
            (status, payload), hints = err(500, f"仿真台内部错误：{e!r}"), []
        ok = status == 200 and payload.get("code") == self.settings["successCode"]
        manual = "【手动】" if hdr.get("x-sim-manual") else ""
        ev.update(tsResp=time.time(), status=status, resp=payload, ok=ok, hints=hints, manual=bool(manual),
                  summary=f"{manual}{request.method} {request.url.path.replace(API, '')} → {status} "
                          f"code={payload.get('code')} {payload.get('message')}{self.lab_detail(key, payload, ok)}")
        self.log.commit(ev)
        return JSONUTF8(payload, status_code=status, headers={"WWW-Authenticate": "Bearer"} if status == 401 else None)

    @staticmethod
    def lab_detail(key, payload, ok):
        if not ok:
            return ""
        d = payload.get("data")
        if key == "workcellstate":
            run = d.get("run") or {}
            return f"；runState={d['runState']}" + (f"，run.name={run.get('name')}" if run else "")
        if key == "devicestates":
            return "；" + "，".join(f"{x['deviceName']}={x['deviceState']}" for x in d)
        if key == "load":
            return "；" + "、".join(x["labwareName"] for x in d)
        if key in ("preview", "run"):
            return f"；taskId={payload.get('taskId')}"
        if key == "unload":
            return f"；uuid {len(d)} 个"
        return ""

    async def labillion_http_error(self, request, status):
        allowed = sorted({m for _, m, p, _, _ in LAB_ROUTES if API + p == request.url.path.rstrip("/")})
        if status == 405:
            message = f"请求方法不支持：{request.url.path} 应使用 {' / '.join(allowed)}（协议文档第 8 章）"
        elif status == 404:
            message = f"接口不存在：{request.url.path}（接口都在 {API}/ 下，说明页 /docs）"
        else:
            message = f"HTTP {status}"
        payload = {"code": status, "message": message, "data": None}
        raw = await request.body()
        ev = self.log.add("labillion", client=request.client.host if request.client else "", method=request.method,
                          path=request.url.path, key="?", headers={"Platform": request.headers.get("platform")},
                          req=decode_for_log(raw))
        ev.update(tsResp=time.time(), status=status, resp=payload, ok=False, hints=[],
                  summary=f"{request.method} {request.url.path} → {status} {message}")
        self.log.commit(ev)
        return JSONUTF8(payload, status_code=status)

    async def rail_dispatch(self, request):
        t0 = time.time()
        raw = await request.body()
        client = request.client.host if request.client else "127.0.0.1"
        body, bad_json = None, None
        if raw.strip():
            try:
                body = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as e:
                bad_json = str(e)
        ev = self.log.add("rail", ts=t0, client=client, method="POST", path=NUC_PATH,
                          req=body if bad_json is None else raw.decode("utf-8", "replace")[:2000])
        ctx = {"hints": [], "final": None}
        if bad_json is not None:
            status, resp = 400, self.rail.env(None, EXEC_ERROR, RC_PARAM, f"参数错误：请求体不是合法 JSON：{bad_json}")
        else:
            try:
                status, resp = 200, await self.rail.handle(body, client, ctx)
            except Exception as e:  # 仿真自己的 bug：回一个错误结果，记录照常落盘
                traceback.print_exc()
                status, resp = 500, self.rail.env(body, EXEC_ERROR, RC_PARAM, f"仿真台内部错误：{e!r}")
        cmd = body.get("Command", "").strip() if isinstance(body, dict) and isinstance(body.get("Command"), str) else "?"
        mode = {1: "同步", 0: "异步"}.get(body.get("CommandSyncAsync") if isinstance(body, dict) else None, "?")
        extra = f" Data={resp['Data']}" if resp.get("Data") is not None else ""
        manual = "【手动】" if request.headers.get("x-sim-manual") else ""
        ev.update(tsResp=time.time(), status=status, resp=resp, hints=ctx["hints"], cmd=cmd, sync=ctx.get("sync"),
                  manual=bool(manual),
                  summary=f"{manual}{cmd}（{mode}）→ ExecuteStatus={resp['ExecuteStatus']} {resp['ResultCode']} "
                          f"{resp['Message']}{extra}")
        self.log.commit(ev)
        if ctx["final"]:
            self.rail.record_result(ctx["final"][0], ctx["final"][1])
        return JSONUTF8(resp, status_code=status)

    async def rail_http_error(self, request, status):
        message = (f"请求方法不支持：NUC 驱动只有 POST {NUC_PATH}" if status == 405 else
                   f"接口不存在：{request.url.path}（NUC 驱动只有 POST {NUC_PATH}，说明页 /docs）")
        resp = self.rail.env(None, EXEC_ERROR, RC_PARAM, message)
        ev = self.log.add("rail", client=request.client.host if request.client else "", method=request.method,
                          path=request.url.path, req=decode_for_log(await request.body()))
        ev.update(tsResp=time.time(), status=status, resp=resp, hints=[], cmd="?",
                  summary=f"{request.method} {request.url.path} → {status} {message}")
        self.log.commit(ev)
        return JSONUTF8(resp, status_code=status)

    # ---- 仿真台页面用

    def snapshot(self, since):
        now = time.time()
        lab, rail, scene = self.lab, self.rail, self.scene
        task = rail.task
        fresh, cursor = self.log.since(since)
        return {
            "now": fmt_ms(now),
            "round": {"id": self.log.round_id, "start": fmt_ms(self.log.round_start),
                      "elapsed": round(now - self.log.round_start, 1),
                      "limit": self.cfg["task1"]["timeLimitMinutes"] * 60},
            "settings": dict(self.settings, speed=self.speed),
            "config": {"platform": lab.cfg["platform"], "platformName": lab.cfg.get("platformName", ""),
                       "tenantId": lab.cfg["tenantId"], "user": lab.cfg["users"][0]["userName"],
                       "password": lab.cfg["users"][0]["password"], "runName": self.cfg["task1"]["runName"],
                       "process": self.cfg["task1"]["process"],
                       "ports": self.cfg["server"], "railDeviceId": rail.cfg["deviceId"],
                       "callbackPort": rail.cfg["callbackPort"],
                       "rail": {"speed": rail.cfg.get("speedMmPerSecond", 200),
                                "tolerance": rail.cfg.get("arrivalToleranceMm", 1.0),
                                "resetSeconds": rail.cfg.get("resetSeconds", 3)},
                       "positions": [{"cursorIndex": i, "name": p["name"], "label": p.get("label", ""), "mm": p["mm"]}
                                     for i, p in sorted(rail.positions.items())]},
            "labillion": {"workcell": lab.workcell_view(), "internal": lab.internal_view(),
                          "maintenance": lab.maintenance, "preview": lab.last_preview,
                          "devices": [dict(lab.device_view(d), state=lab.device_state(d), fault=d["fault"])
                                      for d in lab.devices],
                          "labwares": [dict(lab.labware_view(lw), round=lw["uuid"] in lab.round_loaded)
                                       for lw in lab.labwares],
                          "processes": [dict(lab.process_view(p), steps=len(p["steps"]),
                                             seconds=lab.process_seconds(p["name"])) for p in lab.cfg["processes"]],
                          "errors": [lab.error_view(e) for e in lab.errors], "warnings": lab.warnings},
            "rail": {"state": rail.state, "pausedFrom": rail.paused_from, "fault": rail.fault,
                     "sensor": rail.sensor(), "last": rail.last,
                     "task": {"cmd": task["cmd"], "target": rail.positions[task["targetIndex"]]["name"],
                              "sync": task["sync"], "fault": task["fault"]} if task else None,
                     "error": {"eventId": rail.error["eventId"], "handle": rail.error["handle"]} if rail.error else None},
            "scene": {"e1": scene.e1, "robots": scene.robots, "violations": scene.violations[-30:]},
            "checks": compute_checks(self),
            "demo": self.demo.view(),
            "manual": {"loggedIn": bool(self.manual_token)},
            "events": fresh[-500:],
            "lastSeq": cursor,
        }

    def apply_settings(self, body):
        changed = []
        if "speed" in body:
            s = float(body["speed"])
            if not 0.1 <= s <= 100:
                raise SimError("倍速范围 0.1 ~ 100")
            self.speed = s
            changed.append(f"倍速 {s:g}")
        if "mode" in body:
            if body["mode"] not in MODE_NAMES:
                raise SimError("mode 只能是 competition 或 pujiang")
            self.settings["mode"] = body["mode"]
            changed.append(MODE_NAMES[body["mode"]])
        if "successCode" in body:
            if body["successCode"] not in (0, 200):
                raise SimError("successCode 只能是 200 或 0")
            self.settings["successCode"] = body["successCode"]
            changed.append(f"成功码 {body['successCode']}")
        for k in ("strictHeaders",):
            if k in body:
                self.settings[k] = bool(body[k])
                changed.append(f"{k}={bool(body[k])}")
        for k in ("dataPushUrl", "callbackBaseUrl"):
            if k in body:
                self.settings[k] = str(body[k] or "").strip()
                changed.append(f"{k}={self.settings[k] or '（空）'}")
        if not changed:
            raise SimError("没有可改的设置")
        self.log.note("sim", "设置：" + "，".join(changed))

    def apply_fault(self, body):
        target, kind = body.get("target"), body.get("kind")
        lab = self.lab
        if target == "rail":
            if kind == "clear":
                self.rail.fault = None
                msg = "取消交互口故障"
            elif kind in ("error", "timeout", "offset", "lostcontrol", "estop"):
                self.rail.fault = kind
                msg = {"error": "交互口下一次移动：中途报错（ExecuteStatus=1，进 InError）",
                       "timeout": "交互口下一次移动：卡住后超时（ExecuteStatus=4，进 InError）",
                       "offset": "交互口下一次移动：接口报成功但实际没到位（差几十毫米）",
                       "lostcontrol": "交互口下一次移动：设备失控（DeviceErrorAffect=1，不能重试 / 忽略，只能 Reset）",
                       "estop": "交互口下一次移动：急停（DeviceErrorAffect=2，不能重试 / 忽略，只能 Reset）"}[kind]
            else:
                raise SimError("交互口故障 kind 只能是 error / timeout / offset / lostcontrol / estop / clear")
        elif target == "labillion":
            if kind == "error":
                e = lab.inject_error(body.get("message"), body.get("options"))
                msg = f"注入流程错误：{e['impactedDevice']} {e['errorMessage']}，容错选项 {e['handleOptions']}"
            elif kind == "warning":
                dev = body.get("device") or self.cfg["labillion"]["devices"][0]["name"]
                lab.warnings.append({"uuid": str(uuid.uuid4()), "impactedDevice": dev, "impactedLabwares": "",
                                     "warningMessage": body.get("message") or "报警: 仿真注入"})
                msg = f"注入报警：{dev}"
            elif kind == "clearWarnings":
                lab.warnings = []
                msg = "清除全部报警"
            elif kind in ("timeout", "serverError"):
                code = 408 if kind == "timeout" else 500
                lab.inject_next = [code] * max(1, int(body.get("count", 1)))
                msg = f"接下来 {len(lab.inject_next)} 次 Labillion 请求返回 {code}"
            elif kind == "maintenance":
                lab.maintenance = bool(body.get("on"))
                msg = "Labillion 维护模式（全部接口返回 503）" + ("打开" if lab.maintenance else "关闭")
            elif kind == "deviceFault":
                d = next((x for x in lab.devices if x["cfg"]["name"] == body.get("device")), None)
                if d is None:
                    raise SimError("设备不存在")
                d["fault"] = bool(body.get("on"))
                msg = f"设备 {d['cfg']['name']} 故障（状态 InError）" + ("打开" if d["fault"] else "解除")
            else:
                raise SimError("Labillion 故障 kind 只能是 error / warning / clearWarnings / timeout / serverError / maintenance / deviceFault")
        else:
            raise SimError("target 只能是 rail 或 labillion")
        self.log.note("sim", msg)
        return msg

    def sensor_query(self):
        """仿真相机：智能体在真机上用相机/视觉做“物理到位静止”的独立核验，仿真里用这个接口代替。"""
        s = self.rail.sensor()
        if s["atAndStatic"] and self.rail.sensor_reported != s["atAndStatic"]:
            self.rail.sensor_reported = s["atAndStatic"]
            self.log.note("sensor", f"智能体查询仿真相机：交互口在 {s['atAndStatic']} 到位静止（{s['positionMm']} mm）",
                          position=s["atAndStatic"], positionMm=s["positionMm"], motion=self.rail.motion)
        return s


# ---------------------------------------------------------------- 三个 Web 应用

def build_labillion_app(sim):
    app = FastAPI(title="Labillion 平台接口（仿真）", version="协议文档第 8 章", description=LAB_DESC,
                  docs_url="/docs", redoc_url=None)
    bearer = HTTPBearer(auto_error=False, description="先调 /login，把 data 里的 Token 填这里（自动加 Bearer 前缀）")
    examples = sim.lab.doc_examples()
    platform_default = sim.cfg["labillion"]["platform"]

    def make(key):
        # 默认值只是让本页 Try it out 时自动填上请求头；真正的检查看实际收到的请求头，没带照样报错
        async def endpoint(request: Request,
                           platform: Optional[str] = Header(platform_default, description="平台唯一标识（必填，本页已填好）"),
                           accept_language: Optional[str] = Header("zh-CN", description="语言（必填，本页已填好）"),
                           _cred: Any = Depends(bearer)):
            return await sim.labillion_dispatch(request, key)
        return endpoint

    lab_cfg = sim.cfg["labillion"]
    for key, method, sub, title, req_example in LAB_ROUTES:
        if key == "login":  # 说明页的登录示例直接用 config.json 里的账号
            req_example = {"tenantId": lab_cfg["tenantId"], "userName": lab_cfg["users"][0]["userName"],
                           "password": lab_cfg["users"][0]["password"]}
        extra = None
        if req_example is not None:
            extra = {"requestBody": {"required": key not in ("online", "offline"),
                                     "content": {"application/json": {"example": req_example}}}}
        app.add_api_route(API + sub, make(key), methods=[method], summary=title, operation_id=key,
                          description=LAB_NOTES.get(key, ""), openapi_extra=extra, response_class=JSONUTF8,
                          responses={200: {"description": "业务结果（看 code）",
                                           "content": {"application/json": {"example": examples.get(key)}}}})

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        return await sim.labillion_http_error(request, exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def bad_request(request, exc):
        return JSONUTF8({"code": 400, "message": f"请求参数错误：{exc.errors()}", "data": None}, status_code=400)
    return app


def build_rail_app(sim):
    app = FastAPI(title="交互口滑轨 NUC 驱动（仿真）", version="NUC 文档第 6~7 章", description=RAIL_DESC,
                  docs_url="/docs", redoc_url=None)
    example = {"CommandSyncAsync": 1, "RequestId": "85a57212-b0cf-43bf-a660-8354e5346dcd",
               "DeviceId": sim.cfg["rail"]["deviceId"], "Command": "SlideByCursorIndex",
               "Parameters": {"CursorIndex": 1}}
    resp_example = {"ExecuteStatus": 0, "ResultCode": "00000000", "Message": "Succ", "DetailMessage": None,
                    "Duration": 2000, "RequestId": "85a57212-b0cf-43bf-a660-8354e5346dcd", "EventId": ZERO_UUID,
                    "DeviceId": sim.cfg["rail"]["deviceId"], "Command": "SlideByCursorIndex", "Parameters": {},
                    "Data": None}

    @app.post(NUC_PATH, summary="所有指令都走这一个接口", response_class=JSONUTF8,
              openapi_extra={"requestBody": {"required": True, "content": {"application/json": {"example": example}}}},
              responses={200: {"description": "同步：最终结果；异步：已受理（结果走回调）",
                               "content": {"application/json": {"example": resp_example}}}})
    async def post(request: Request):
        return await sim.rail_dispatch(request)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request, exc):
        return await sim.rail_http_error(request, exc.status_code)
    return app


def build_console_app(sim):
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    async def body_of(request):
        raw = await request.body()
        try:
            return json.loads(raw.decode("utf-8")) if raw.strip() else {}
        except (UnicodeDecodeError, ValueError):
            raise SimError("请求体不是 JSON")

    def guarded(fn):
        async def wrapper(request: Request):
            try:
                return JSONUTF8({"ok": True, "result": await fn(await body_of(request))})
            except SimError as e:
                return JSONUTF8({"ok": False, "error": str(e)}, status_code=400)
        return wrapper

    @app.get("/")
    async def index():
        return FileResponse(HERE / "dashboard.html", media_type="text/html; charset=utf-8")

    @app.get("/sim/state")
    async def state(since: int = 0):
        return JSONUTF8(sim.snapshot(since))

    @app.get("/sim/port")
    async def port():
        return JSONUTF8(sim.sensor_query())

    @app.get("/sim/export")
    async def export():
        text = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in sim.log.events if e["seq"] not in sim.log.pending)
        return Response(text.encode("utf-8"), media_type="application/x-ndjson; charset=utf-8",
                        headers={"Content-Disposition": f"attachment; filename=server_events_{sim.log.round_id}.jsonl"})

    async def reset(body):
        sim.new_round(body.get("mode"))
        return sim.log.round_id

    async def settings(body):
        sim.apply_settings(body)
        return sim.settings

    async def fault(body):
        return sim.apply_fault(body)

    async def robot(body):
        return sim.scene.robot_action(body.get("robot"), body.get("action"), body.get("object"))

    async def demo_start(body):
        return sim.demo.start(body.get("scenario", "all"), bool(body.get("faults")), bool(body.get("async")))

    async def demo_stop(body):
        return sim.demo.stop()

    async def manual(body):
        return await sim.manual(body)

    for path, fn in (("/sim/reset", reset), ("/sim/settings", settings), ("/sim/fault", fault),
                     ("/sim/robot", robot), ("/sim/demo", demo_start), ("/sim/demo/stop", demo_stop),
                     ("/sim/manual", manual)):
        app.add_api_route(path, guarded(fn), methods=["POST"])
    return app


# ---------------------------------------------------------------- 启动

class SharedServer(uvicorn.Server):
    """三个端口在一个进程里：Ctrl+C 由 main 统一处理，不让每个 Server 各自抢信号。"""

    @contextlib.contextmanager
    def capture_signals(self):
        yield

    def install_signal_handlers(self):  # 老版本 uvicorn 用这个名字
        pass


def lan_ip():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("10.255.255.255", 1))
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def port_free(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False


def sim_already_running(console_port):
    """端口被占时，看看占着的是不是另一个仿真台（最常见：上次开的窗口还没关）。"""
    url = f"http://127.0.0.1:{console_port}/sim/state?since=999999999"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=1.5) as r:
            return "round" in json.loads(r.read().decode("utf-8"))
    except (OSError, ValueError):
        return False


async def open_page(sim, servers, page_url, run_demo):
    """等三个端口都起来，打开状态页；--demo 时再让示例智能体跑一遍，跑完仿真台继续开着。"""
    while not all(s.started for s in servers):
        if any(s.should_exit for s in servers):
            return
        await asyncio.sleep(0.1)
    if sys.platform in ("darwin", "win32"):  # Linux 服务器上多半没有桌面，只打印地址
        await asyncio.to_thread(webbrowser.open, page_url)
    if not run_demo:
        return
    await asyncio.sleep(2)  # 等页面打开，好看着状态一步步变
    print(f"\n--demo：状态页 {page_url}，示例智能体开始跑（约 20 秒）……\n", flush=True)
    sim.demo.echo = True
    sim.demo.start("all")
    while sim.demo.running():
        await asyncio.sleep(0.2)
    if not any(s.should_exit for s in servers):
        print(f"\n示例智能体跑完了{'' if sim.demo.rc == 0 else '，有没通过的（看上面）'}。仿真台继续开着，"
              f"页面 {page_url}，按 Ctrl+C 退出。", flush=True)


async def serve_all(sim, servers, page_url, run_demo):
    def stop(*_):
        for s in servers:
            if s.should_exit:
                s.force_exit = True
            s.should_exit = True
    signal.signal(signal.SIGINT, stop)
    with contextlib.suppress(AttributeError, ValueError):
        signal.signal(signal.SIGTERM, stop)
    loop = asyncio.get_running_loop()
    ticker = loop.create_task(sim.loop())
    opener = loop.create_task(open_page(sim, servers, page_url, run_demo)) if page_url else None
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        ticker.cancel()
        if opener:
            opener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await opener
        with contextlib.suppress(SimError):
            sim.demo.stop()  # 仿真台退出时，正在跑的示例智能体也停掉


def main():
    ap = argparse.ArgumentParser(description="镁伽 Labillion + 交互口 E1 仿真台")
    ap.add_argument("--config", default=str(HERE / "config.json"), help="配置文件，默认同目录 config.json")
    ap.add_argument("--speed", type=float, default=1.0, help="仿真倍速：流程运行、设备上线、滑轨移动都按这个倍数加快")
    ap.add_argument("--quiet", action="store_true", help="终端不逐条打印调用记录")
    ap.add_argument("--demo", action="store_true",
                    help="开仿真台后自动打开状态页，让示例智能体把三条流程跑一遍；跑完仿真台继续开着")
    ap.add_argument("--no-browser", action="store_true", help="启动时不自动打开状态页")
    args = ap.parse_args()
    for stream in (sys.stdout, sys.stderr):  # Windows 把输出重定向到文件时编码可能不是 UTF-8，打印不能把请求处理搞崩
        with contextlib.suppress(AttributeError, ValueError):
            stream.reconfigure(errors="replace")
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    srv = cfg["server"]
    names = [("Labillion 接口", srv["labillionPort"]), ("交互口滑轨 NUC 驱动", srv["railPort"]),
             ("仿真台页面", srv["consolePort"])]
    busy = [f"{n}（{p}）" for n, p in names if not port_free(srv["host"], p)]
    if busy:
        if sim_already_running(srv["consolePort"]):
            sys.exit(f"已经有一个仿真台开着（页面 http://127.0.0.1:{srv['consolePort']}/），不用再开。"
                     f"要重开：先在它的终端窗口按 Ctrl+C；找不到那个窗口，Mac / Linux 运行 pkill -f sim_server.py")
        sys.exit(f"端口被占用：{'、'.join(busy)}。关掉占用的程序，或在 config.json 的 server 里换端口。")
    sim = Sim(cfg, args.speed, args.quiet or args.demo, str(Path(args.config).resolve()))
    tls = {}
    if srv.get("labillionCertFile") and srv.get("labillionKeyFile"):
        tls = {"ssl_certfile": str(HERE / srv["labillionCertFile"]), "ssl_keyfile": str(HERE / srv["labillionKeyFile"])}
    apps = [(build_labillion_app(sim), srv["labillionPort"], tls), (build_rail_app(sim), srv["railPort"], {}),
            (build_console_app(sim), srv["consolePort"], {})]
    servers = [SharedServer(uvicorn.Config(app, host=srv["host"], port=port, log_level="warning",
                                           access_log=False, **extra)) for app, port, extra in apps]
    ip = lan_ip()
    scheme = "https" if tls else "http"
    print("=" * 72)
    print("镁伽 Labillion + 交互口 E1 仿真台已启动（Ctrl+C 退出）")
    print(f"  仿真台页面          http://{ip}:{srv['consolePort']}/   （本机也可用 127.0.0.1）")
    print(f"  Labillion 接口      {scheme}://{ip}:{srv['labillionPort']}{API}/...   说明页 /docs")
    print(f"  交互口 NUC 驱动     http://{ip}:{srv['railPort']}{NUC_PATH}   说明页 /docs")
    print(f"  Platform={cfg['labillion']['platform']}  账号 {cfg['labillion']['users'][0]['userName']}"
          f"  runName={cfg['task1']['runName']}")
    print(f"  异步回调发到 http://<客户端IP>:{cfg['rail']['callbackPort']}/api/OnResponse | /api/OnError")
    print(f"  调用记录写在 {HERE / 'logs'}")
    print("  页面上可以：运行示例智能体、手动一步步调接口、重置到初始状态、注入故障")
    print("=" * 72, flush=True)
    page_url = None if args.no_browser and not args.demo else f"http://127.0.0.1:{srv['consolePort']}/"
    asyncio.run(serve_all(sim, servers, page_url, args.demo))


if __name__ == "__main__":
    main()
