#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""示例智能体：照真机的方式调仿真台的接口，跑通三条流程，并留下带时间戳的请求-响应记录。

  python3 demo_agent.py t1    任务1 设备接入：登录 → 上线 → 耗材登记 → 预览 → 运行 MEGA_STEP_TEST → 确认 Running
                              → 暂停 → 继续 → 终止 → 确认 NotRunning → 卸载耗材并确认消失 → 下线
  python3 demo_agent.py e1    交互口 E1（比赛规则）：空载弹出 OUT → 到位静止核验 → 机器人放入 / 取回 → 空载收回 IN → 核验
  python3 demo_agent.py flow  浦江调用流程：滑轨载物上料 ×2 → 运行到正常结束 → 收数据 → 滑轨载物下料 ×2
  python3 demo_agent.py all   三条依次跑：flow 单独一轮；t1 新开一轮，e1 接着 t1 这一轮跑，
                              跑完状态页停在这一轮，任务1 检查点和 B-L / B-C 都是绿的
（Windows 把 python3 换成 python。也可以让仿真台代劳：python3 sim_server.py --demo）

常用选项：--host 仿真台所在机器（默认 127.0.0.1）  --async 交互口用异步模式（结果走回调）
         --faults e1 里顺带演示故障处理（中途报错 → Retry、报成功但没到位 → 重发）
记录：logs/agent_records_<场景>_<时间>.jsonl，一行一次请求：请求时间、方法、地址、请求头、请求体、响应时间、状态码、响应体。

标【仿真专用】的调用（重置、机器人动作、仿真相机）只在仿真台有；真机上换成机器人自己的控制和相机核验。
"""
import argparse
import datetime
import json
import socket
import sys
import threading
import time
import uuid
import warnings
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

warnings.filterwarnings("ignore", message=".*OpenSSL.*")  # Mac 自带 Python 3.9 用 LibreSSL，urllib3 会提示，不影响局域网 HTTP
try:
    import requests
except ImportError:
    sys.exit("缺少依赖 requests。先在这个文件夹里运行：python3 -m pip install -r requirements.txt（Windows 用 python）")

HERE = Path(__file__).resolve().parent
SUCCESS_CODES = (200, 0)  # 协议文档 7.1 写“0 表示成功”，各接口示例写的是 200，两种都认


class Fail(Exception):
    pass


def now_ms():
    return datetime.datetime.now().astimezone().isoformat(timespec="milliseconds")


def need(cond, what):
    if not cond:
        raise Fail(what)


def wait_until(desc, fn, timeout=60.0, interval=0.5):
    """轮询到 fn() 返回真值；轮询间隔别太小（仿真台限流：同一客户端每秒 20 次）。"""
    deadline = time.time() + timeout
    while True:
        value = fn()
        if value:
            return value
        if time.time() > deadline:
            raise Fail(f"等待超时：{desc}")
        time.sleep(interval)


def say(text):
    print(text, flush=True)


class Recorder:
    """把每次请求和响应连同时间戳写成一行 JSON。"""

    def __init__(self, scenario):
        (HERE / "logs").mkdir(exist_ok=True)
        self.path = HERE / "logs" / f"agent_records_{scenario}_{datetime.datetime.now():%Y%m%d_%H%M%S}.jsonl"
        self.fh = open(self.path, "w", encoding="utf-8")
        self.n = 0
        self.lock = threading.Lock()

    def write(self, rec):
        with self.lock:
            self.n += 1
            self.fh.write(json.dumps(dict(seq=self.n, **rec), ensure_ascii=False) + "\n")
            self.fh.flush()


class Http:
    def __init__(self, rec):
        self.session = requests.Session()
        self.session.trust_env = False  # 局域网地址，不走系统代理
        self.rec = rec

    def call(self, step, method, url, headers=None, body=None, timeout=120):
        t0 = now_ms()
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        try:
            r = self.session.request(method, url, headers=headers, data=data, timeout=timeout)
            status = r.status_code
            try:
                resp = r.json()
            except ValueError:
                resp = r.text
        except requests.RequestException as e:
            status, resp = None, f"请求失败：{e}"
        self.rec.write({"step": step, "request": {"time": t0, "method": method, "url": url,
                                                  "headers": headers or {}, "body": body},
                        "response": {"time": now_ms(), "status": status, "body": resp}})
        return status, resp


class Labillion:
    """协议文档第 8 章：每个请求都带 Content-Type、Accept-Language、Platform，登录后再带 Authorization。"""

    def __init__(self, http, base, cfg):
        self.http, self.base, self.cfg = http, base, cfg
        self.token = None

    def call(self, step, method, path, body=None):
        headers = {"Content-Type": "application/json", "Accept-Language": "zh-CN", "Platform": self.cfg["platform"]}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        status, resp = self.http.call(step, method, self.base + path, headers, body)
        ok = status == 200 and isinstance(resp, dict) and resp.get("code") in SUCCESS_CODES
        detail = ""
        if ok and path == "/workcellstate":
            d = resp["data"]
            detail = f" runState={d['runState']}" + (f" run.name={d['run']['name']}" if d.get("run") else "")
        elif isinstance(resp, dict) and not ok:
            detail = f" {resp.get('message')}"
        say(f"  [{step}] {method} {path} → {status} code={resp.get('code') if isinstance(resp, dict) else '-'}"
            f"{detail}  {'OK' if ok else '失败'}")
        return ok, resp

    def login(self):
        user = self.cfg["users"][0]
        ok, r = self.call("登录", "POST", "/login", {"tenantId": self.cfg["tenantId"], "userName": user["userName"],
                                                     "password": user["password"]})
        need(ok, f"登录失败：{r}")
        self.token = r["data"]

    def run_state(self, step):
        """返回 /workcellstate 的 data；调用失败返回空字典（调用方用 .get 取字段）。"""
        ok, r = self.call(step, "GET", "/workcellstate")
        return r["data"] if ok else {}

    def online_and_wait(self, tag):
        ok, _ = self.call(f"{tag}上线设备", "POST", "/onlinedevices")
        need(ok, "上线设备失败")

        def all_idle():
            ok, r = self.call(f"{tag}确认设备都是 Idle", "GET", "/devicestates")
            return ok and all(d["deviceState"] == "Idle" for d in r["data"])
        wait_until("设备全部 Idle", all_idle, timeout=60, interval=1.0)

    def free_locations(self, tag, n):
        ok, r = self.call(f"{tag}查询设备", "GET", "/devices")
        need(ok, "查询设备失败")
        locations = [loc for d in r["data"] for loc in d["locations"] if loc.startswith("Carousel")]
        ok, r = self.call(f"{tag}查询耗材（看哪些位置空着）", "GET", "/labwares")
        used = {lw["location"] for lw in r["data"]}
        free = [loc for loc in locations if loc not in used]
        need(len(free) >= n, "空位置不够")
        return free[:n]


class Callbacks:
    """异步模式的回调接收端（NUC 文档第 6 章：/api/OnResponse 成功、/api/OnError 失败），顺带接 Labillion 的数据推送。"""

    def __init__(self, port, rec):
        self.results, self.data = {}, []
        self.cond = threading.Condition()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                try:
                    body = json.loads(raw.decode("utf-8"))
                except ValueError:
                    body = raw.decode("utf-8", "replace")
                rec.write({"step": f"收到回调 {self.path}", "callback": {"time": now_ms(), "path": self.path, "body": body}})
                with outer.cond:
                    if self.path in ("/api/OnResponse", "/api/OnError") and isinstance(body, dict):
                        outer.results[body.get("RequestId")] = (self.path, body)
                    else:
                        outer.data.append(body)
                    outer.cond.notify_all()
                out = b'{"code":200,"message":"ok"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

            def log_message(self, *args):
                pass
        try:
            self.server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        except OSError:
            sys.exit(f"回调端口 {port} 被占用，关掉占用的程序再试")
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def wait(self, request_id, timeout=120):
        with self.cond:
            ok = self.cond.wait_for(lambda: request_id in self.results, timeout)
            if not ok:
                raise Fail(f"等回调超时：RequestId={request_id}（仿真台页面的调用记录里看回调有没有发出去）")
            return self.results.pop(request_id)


class Rail:
    """交互口滑轨 NUC 驱动：所有指令 POST 到同一个地址。"""

    def __init__(self, http, url, cfg, callbacks):
        self.http, self.url, self.cfg, self.callbacks = http, url, cfg, callbacks
        self.idx = {p["name"]: int(p["cursorIndex"]) for p in cfg["positions"]}

    def cmd(self, step, command, params=None, event_id=None):
        body = {"CommandSyncAsync": 0 if self.callbacks else 1, "RequestId": str(uuid.uuid4()),
                "DeviceId": self.cfg["deviceId"], "Command": command, "Parameters": params or {}}
        if event_id:
            body["EventId"] = event_id
        status, resp = self.http.call(step, "POST", self.url, {"Content-Type": "application/json"}, body)
        need(isinstance(resp, dict) and "ExecuteStatus" in resp, f"交互口没响应：{resp}")
        if self.callbacks and resp["ExecuteStatus"] == 0:  # 异步：这只是“已受理”，最终结果等回调
            path, resp = self.callbacks.wait(body["RequestId"])
        ok = resp["ExecuteStatus"] == 0 and resp["ResultCode"] == "00000000"
        extra = f" Data={resp['Data']}" if resp.get("Data") is not None else ""
        say(f"  [{step}] {command}{'（异步）' if self.callbacks else ''} → ExecuteStatus={resp['ExecuteStatus']} "
            f"{resp['ResultCode']} {resp['Message']}{extra}  {'OK' if ok else '失败'}")
        return ok, resp

    def state(self, step):
        ok, r = self.cmd(step, "GetState")
        need(ok, "GetState 失败")
        return r["Data"]

    def online(self, tag=""):
        if self.state(f"{tag}查询交互口状态") != "Idle":
            ok, r = self.cmd(f"{tag}交互口上线 Reset", "Reset")
            need(ok, f"Reset 失败：{r}")
        need(self.state(f"{tag}确认交互口 Idle") == "Idle", "Reset 后不是 Idle")


class Console:
    """【仿真专用】仿真台接口：重置、机器人动作、仿真相机。"""

    def __init__(self, http, base):
        self.http, self.base = http, base

    def post(self, step, path, body):
        status, resp = self.http.call(f"【仿真专用】{step}", "POST", self.base + path,
                                      {"Content-Type": "application/json"}, body)
        need(isinstance(resp, dict) and resp.get("ok"), f"{step} 失败：{resp}")
        say(f"  [【仿真专用】{step}] {resp.get('result') if isinstance(resp.get('result'), str) else 'OK'}")
        return resp["result"]

    def reset(self, mode):
        self.post(f"重置仿真台（{mode}）", "/sim/reset", {"mode": mode})

    def settings(self, **kw):
        self.post("仿真台设置", "/sim/settings", kw)

    def robot(self, name, action):
        self.post(f"机器人 {name} {action}", "/sim/robot", {"robot": name, "action": action})

    def camera(self, step):
        status, resp = self.http.call(f"【仿真专用】{step}", "GET", self.base + "/sim/port")
        need(status == 200, "仿真相机查询失败")
        return resp

    def state(self):
        r = self.http.session.get(self.base + "/sim/state", params={"since": 10 ** 9}, timeout=10)
        return r.json()


def verify_port(ctx, name, tag):
    """到位核验：等仿真相机看到交互口在指定点位且静止，再 GetState 确认 Idle（API 成功不等于机械到位）。"""
    def seen():
        cam = ctx.con.camera(f"{tag}仿真相机看交互口")
        return cam if cam["atAndStatic"] == name else None
    cam = wait_until(f"交互口在 {name} 到位静止", seen, timeout=10, interval=0.3)
    say(f"  [{tag}到位核验] 仿真相机：交互口在 {name}，{cam['positionMm']} mm，静止")
    need(ctx.rail.state(f"{tag}到位核验 GetState") == "Idle", "到位后交互口不是 Idle")


def slide(ctx, name, tag):
    ok, r = ctx.rail.cmd(f"{tag}交互口移动到 {name}", "SlideByCursorIndex", {"CursorIndex": ctx.rail.idx[name]})
    need(ok, f"交互口移动到 {name} 失败：{r}")
    verify_port(ctx, name, tag)


# ---------------------------------------------------------------- 三个场景

def scenario_t1(ctx, reset):
    lab, cfg = ctx.lab, ctx.cfg
    say("\n=== 任务1 设备接入：从登录到下线的 20 个接口检查点 ===")
    if reset:
        ctx.con.reset("competition")
    lab.login()
    lab.online_and_wait("")
    ok, r = lab.call("查询耗材类型", "GET", "/labwaretypes")
    need(ok, "查询耗材类型失败")
    free = lab.free_locations("", 2)
    body = [{"labwareType": "384 Deep Well Microplate", "lidType": "Microplate Lid", "label": "t1", "location": free[0]},
            {"labwareType": "96 Well Microplate", "label": "t1", "location": free[1]}]
    ok, r = lab.call("加载耗材", "POST", "/lhdlabwares", body)
    need(ok, "加载耗材失败")
    mine = {lw["labwareName"]: lw["uuid"] for lw in r["data"]}
    say(f"  本轮测试耗材：{mine}")
    ok, r = lab.call("确认耗材已登记", "GET", "/labwares")
    need(set(mine.values()) <= {lw["uuid"] for lw in r["data"]}, "加载的耗材没在 /labwares 里")
    ok, r = lab.call("查询流程", "GET", "/processdesigns")
    need(any(p["name"] == ctx.args.process and p["isValid"] == "True" for p in r["data"]),
         f"流程 {ctx.args.process} 不存在或不合法")
    run_data = [{"process": ctx.args.process, "iterationCount": "1"}]
    ok, r = lab.call("预览", "POST", "/preview", {"Data": run_data})
    need(ok, "预览失败")
    need(lab.run_state("启动前确认 NotRunning").get("runState") == "NotRunning", "启动前平台不是 NotRunning")
    lab.call("查询错误", "GET", "/errors")
    lab.call("查询报警", "POST", "/warnings")
    ok, r = lab.call("运行", "POST", "/run", {"runName": cfg["task1"]["runName"], "data": run_data})
    need(ok, f"运行失败：{r}")

    def running():
        d = lab.run_state("T1-B-13 确认正在运行")
        return d.get("runState") == "Running" and (d.get("run") or {}).get("name") == cfg["task1"]["runName"]
    wait_until("Running 且 run.name 正确", running, timeout=30)
    ok, _ = lab.call("T1-B-14 提交暂停", "POST", "/pause")
    need(ok, "暂停失败")
    lab.run_state("暂停后再查：仍是 Running，接口没有 Paused 字段")
    ok, _ = lab.call("T1-B-15 提交继续", "POST", "/continue")
    need(ok, "继续失败")
    ok, _ = lab.call("T1-B-16 提交终止", "POST", "/stop")
    need(ok, "终止失败")
    wait_until("NotRunning", lambda: lab.run_state("T1-B-17 确认停止").get("runState") == "NotRunning", timeout=30)
    ok, r = lab.call("T1-B-18 卸载测试耗材", "DELETE", "/lhdlabwares", list(mine))
    need(ok and set(r["data"]) == set(mine.values()), f"卸载失败或返回的 uuid 不对：{r}")
    ok, r = lab.call("T1-B-19 确认卸载结果", "GET", "/labwares")
    need(ok and not (set(mine.values()) & {lw["uuid"] for lw in r["data"]}), "卸载的耗材还在 /labwares 里")
    ok, _ = lab.call("T1-B-20 下线设备", "POST", "/offlinedevices")
    need(ok, "下线失败")

    c = ctx.con.state()["checks"]
    say(f"\n仿真台判分：官方明细 T1-B-13~20 通过 {c['score']['official']}/{c['score']['officialTotal']}，"
        f"推测前置项 {c['score']['pre']}/{c['score']['preTotal']}")
    for x in c["official"]:
        say(f"  {'通过' if x['pass'] else '未过'}  {x['id']} {x['name']}  {x['hint'] if not x['pass'] else ''}")
    return c["score"]["official"] == c["score"]["officialTotal"]


def scenario_e1(ctx, reset):
    con, rail = ctx.con, ctx.rail
    say("\n=== 交互口 E1（比赛规则：OUT/IN 运动时必须空载，机器人都在设备运动区外）===")
    if reset:
        con.reset("competition")
    rail.online()

    def clear_to_move(tag):
        cam = con.camera(f"{tag}运动前确认 E1 为空、机器人都在运动区外")
        need(not cam["e1"] and not cam["robotsInZone"], f"不满足空载运动条件：{cam}")

    clear_to_move("B-C01 ")
    slide(ctx, "OUT", "B-C01 ")
    con.robot("B", "enter")      # B-R04：接收机进入设备运动区，把物件放到 E1 并松开
    con.robot("B", "place")
    con.robot("B", "take")       # B-R05：取回同一物件，携物退出
    con.robot("B", "exit")
    clear_to_move("B-C02 ")
    slide(ctx, "IN", "B-C02 ")
    con.robot("B", "deliver")    # B-R06：交互口确认 IN 到位静止后，再把物件送到终点 R 位

    if ctx.args.faults:
        say("\n--- 故障演示 1：移动中途报错 → 用失败结果里的 EventId 重试 ---")
        con.post("设置故障：下一次移动中途报错", "/sim/fault", {"target": "rail", "kind": "error"})
        ok, r = rail.cmd("交互口移动到 OUT（会失败）", "SlideByCursorIndex", {"CursorIndex": rail.idx["OUT"]})
        need(not ok and r["Parameters"].get("HandleCommands"), "应该失败并给出容错选项")
        say(f"  失败 EventId={r['EventId']}，可选 {r['Parameters']['HandleCommands']}，设备状态 {rail.state('查询状态')}")
        ok, r = rail.cmd("重试 Retry（带 EventId）", "Retry", event_id=r["EventId"])
        need(ok, f"重试失败：{r}")
        verify_port(ctx, "OUT", "重试后")
        say("\n--- 故障演示 2：接口报成功，但相机看到没到位 → 重发一次 ---")
        slide(ctx, "IN", "")
        con.post("设置故障：下一次移动报成功但没到位", "/sim/fault", {"target": "rail", "kind": "offset"})
        ok, r = rail.cmd("交互口移动到 OUT（报成功但没到位）", "SlideByCursorIndex", {"CursorIndex": rail.idx["OUT"]})
        need(ok, "这一步接口应报成功")
        time.sleep(1.0)
        cam = con.camera("核验：仿真相机看交互口")
        say(f"  仿真相机：{cam['positionMm']} mm，at={cam['at']} —— 接口成功不等于到位，重发移动指令")
        need(cam["atAndStatic"] != "OUT", "这里应该没到位")
        slide(ctx, "OUT", "重发后")
        slide(ctx, "IN", "")

    st = ctx.con.state()
    c, v = st["checks"], st["scene"]["violations"]
    for x in c["bc"]:
        say(f"  {'通过' if x['pass'] else '未过'}  {x['id']} {x['name']}  完成 {x['count']} 次  {x['hint']}")
    say(f"  联锁违规：{len(v)} 次")
    return all(x["pass"] for x in c["bc"]) and not v


def scenario_flow(ctx, reset):
    con = ctx.con
    say("\n=== 浦江调用流程：上料 → 运行 → 数据 → 下料 ===")
    if reset:
        con.reset("pujiang")
    con.settings(speed=ctx.args.flow_speed, dataPushUrl=f"http://{ctx.my_ip}:{ctx.cfg['rail']['callbackPort']}/api/ReceiveData")
    try:
        return run_flow(ctx)
    finally:  # 还原倍速和推送地址：后面的场景按 1 倍速跑，也别往已经关掉的回调端口推数据
        try:
            con.settings(speed=1, dataPushUrl="")
        except (Fail, requests.RequestException):
            pass


def run_flow(ctx):
    con, lab, rail = ctx.con, ctx.lab, ctx.rail
    lab.login()
    lab.online_and_wait("")
    rail.online()
    objects = ctx.cfg["scene"]["objects"][:2]
    free = lab.free_locations("", len(objects))
    names = []
    for obj, loc in zip(objects, free):
        say(f"--- 上料 {obj} ---")
        slide(ctx, "OUT", "上料 ")
        con.robot("A", "enter")       # 具身放耗材（仿真机器人代替）
        con.robot("A", "place")
        con.robot("A", "exit")
        slide(ctx, "IN", "上料 ")
        ok, r = lab.call("上料 加载耗材", "POST", "/lhdlabwares",
                         [{"labwareType": "96 Well Microplate", "label": obj, "location": loc}])
        need(ok, f"加载耗材失败：{r}")
        names.append(r["data"][0]["labwareName"])
        wait_until("加载完成", lambda: lab.run_state("上料 查询平台状态，判断是否执行完成").get("runState") == "NotRunning")
    ok, r = lab.call("运行流程", "POST", "/run", {"runName": "PUJIANG_FLOW",
                                                 "data": [{"process": ctx.args.process, "iterationCount": "1"}]})
    need(ok, f"运行失败：{r}")
    wait_until("流程运行完成", lambda: lab.run_state("查询平台状态，判断是否执行完成").get("runState") == "NotRunning",
               timeout=600, interval=1.0)
    with ctx.callbacks.cond:
        ctx.callbacks.cond.wait_for(lambda: ctx.callbacks.data, 5)
    say(f"  收到 Labillion 推送的数据：{len(ctx.callbacks.data)} 条"
        + (f"，run={ctx.callbacks.data[-1].get('runName')}" if ctx.callbacks.data else "（没收到）"))
    for name in names:
        say(f"--- 下料 {name} ---")
        slide(ctx, "IN", "下料 ")
        ok, r = lab.call("下料 卸载耗材", "DELETE", "/lhdlabwares", [name])
        need(ok, f"卸载失败：{r}")
        wait_until("卸载完成", lambda: lab.run_state("下料 查询平台状态，判断是否执行完成").get("runState") == "NotRunning")
        slide(ctx, "OUT", "下料 ")
        con.robot("A", "enter")       # 具身取耗材
        con.robot("A", "take")
        con.robot("A", "exit")
        con.robot("A", "deliver")     # 放到终点，手空出来取下一件
    ok, _ = lab.call("下线设备", "POST", "/offlinedevices")
    p = con.state()["checks"]["pujiang"]
    say(f"  浦江流程：上料 {p['loaded']} 件，运行 {p['run']}，{p['dataPush']}，取走 {len(p['takenOut'])} 件，"
        f"整体结束={'是' if p['finished'] else '否'}")
    return p["finished"] and bool(ctx.callbacks.data)


# ---------------------------------------------------------------- 入口

class Ctx:
    pass


def local_ip_towards(host):
    """仿真台看到的本机 IP（给数据推送地址用）。"""
    if host in ("127.0.0.1", "localhost"):
        return "127.0.0.1"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect((host, 1))
        return s.getsockname()[0]


def main():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default=str(HERE / "config.json"))
    cfg_path = pre.parse_known_args()[0].config
    cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
    ap = argparse.ArgumentParser(description="示例智能体：跑通任务1 / 交互口 E1 / 浦江调用流程")
    ap.add_argument("scenario", choices=["t1", "e1", "flow", "all"])
    ap.add_argument("--config", default=cfg_path, help="仿真台用的配置文件（端口、账号、点位从这里读），默认同目录 config.json")
    ap.add_argument("--host", default="127.0.0.1", help="仿真台所在机器的 IP")
    ap.add_argument("--async", dest="use_async", action="store_true", help="交互口指令用异步模式（结果走回调）")
    ap.add_argument("--faults", action="store_true", help="e1 场景里顺带演示故障处理")
    ap.add_argument("--process", default=cfg["task1"]["process"], help="运行的流程名（默认取 config.json 的 task1.process）")
    ap.add_argument("--flow-speed", type=float, default=20, help="flow 场景的仿真倍速（流程 100 秒，20 倍速 5 秒跑完）")
    ap.add_argument("--no-reset", action="store_true", help="不重置仿真台（接着当前状态跑）")
    args = ap.parse_args()

    ports = cfg["server"]
    ctx = Ctx()
    ctx.args, ctx.cfg = args, cfg
    ctx.rec = Recorder(args.scenario)
    http = Http(ctx.rec)
    scheme = "https" if ports.get("labillionCertFile") else "http"
    ctx.lab = Labillion(http, f"{scheme}://{args.host}:{ports['labillionPort']}/api/automation/v1", cfg["labillion"])
    if scheme == "https":
        http.session.verify = False  # 仿真台用的是自签名证书
    need_cb = args.use_async or args.scenario in ("flow", "all")
    ctx.callbacks = Callbacks(cfg["rail"]["callbackPort"], ctx.rec) if need_cb else None
    ctx.rail = Rail(http, f"http://{args.host}:{ports['railPort']}/api/BusFrame/System/Post", cfg["rail"],
                    ctx.callbacks if args.use_async else None)
    ctx.con = Console(http, f"http://{args.host}:{ports['consolePort']}")
    ctx.my_ip = local_ip_towards(args.host)

    # (场景, 跑之前是否重置)。all：flow 单独一轮（浦江模式）；t1 新开一轮，e1 接着这一轮跑，
    # 跑完状态页停在这一轮，任务1 检查点和 B-L / B-C 都在同一页上
    plan = {"t1": [(scenario_t1, True)], "e1": [(scenario_e1, True)], "flow": [(scenario_flow, True)],
            "all": [(scenario_flow, True), (scenario_t1, True), (scenario_e1, False)]}[args.scenario]
    results = []
    try:
        for fn, reset in plan:
            try:
                results.append((fn.__name__, fn(ctx, reset and not args.no_reset)))
            except Fail as e:
                say(f"\n!! 中断：{e}")
                results.append((fn.__name__, False))
    except requests.ConnectionError:
        sys.exit(f"连不上仿真台 {args.host}：先启动仿真台（python3 sim_server.py），或用 --host 指定它的 IP")
    except KeyboardInterrupt:
        sys.exit("\n已中断")
    say("\n=== 结果 ===")
    for name, ok in results:
        say(f"  {name.replace('scenario_', ''):5s} {'通过' if ok else '没通过'}")
    say(f"带时间戳的请求-响应记录：{ctx.rec.path.relative_to(HERE)}（{ctx.rec.n} 条）")
    sys.exit(0 if all(ok for _, ok in results) else 1)


if __name__ == "__main__":
    main()
