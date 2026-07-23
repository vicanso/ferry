#!/usr/bin/env python3
"""把 ferry 队列里的报文打印成人类可读的形式。

请求和响应的 body 都按 `body_encoding` 处理:`utf8`(或缺省)是原文、直接展示,`base64`
才解码(二进制给十六进制预览)。所以对文本报文这脚本基本不用做事,原文本来就可读。

用法:
    scripts/ferry-dump.py req demo          # 看 bridge:req:demo 的积压
    scripts/ferry-dump.py resp <instance>   # 看某个实例的回复队列
    scripts/ferry-dump.py --raw-key bridge:req:demo
"""

import argparse
import base64
import json
import subprocess
import sys


def redis_lrange(key, limit):
    out = subprocess.run(
        ["redis-cli", "LRANGE", key, "0", str(limit - 1)],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        sys.exit(f"redis-cli 失败: {out.stderr.strip()}")
    return [line for line in out.stdout.splitlines() if line.strip()]


def decode_base64_body(b64):
    """base64 body → (展示用文本, 说明)。二进制给十六进制预览,不强行解码。"""
    if not b64:
        return "", "空"
    raw = base64.b64decode(b64)
    try:
        return raw.decode("utf-8"), f"{len(raw)} 字节 UTF-8"
    except UnicodeDecodeError:
        preview = " ".join(f"{b:02x}" for b in raw[:16])
        hint = " (gzip)" if raw[:2] == b"\x1f\x8b" else ""
        return f"<二进制 {preview}...>", f"{len(raw)} 字节二进制{hint}"


def decode_body_field(obj):
    """按 body_encoding 解读带 body 的对象(请求 / 响应 Ok 通用)。
    utf8(或缺省)是原文直取,base64 才解码。"""
    body = obj.get("body") or ""
    if obj.get("body_encoding", "utf8") == "base64":
        return decode_base64_body(body)
    if not body:
        return "", "空"
    return body, f"{len(body.encode('utf-8'))} 字节 UTF-8(原文)"


def render(msg):
    """就地把 body 字段换成可读形式,并附一行尺寸说明。"""
    if "body" in msg:  # 请求
        msg["body"], note = decode_body_field(msg)
        msg["_body_info"] = note
    result = msg.get("result")
    if isinstance(result, dict):  # 响应:只有 Ok 带 body
        ok = result.get("Ok")
        if isinstance(ok, dict) and "body" in ok:
            ok["body"], note = decode_body_field(ok)
            ok["_body_info"] = note
    return msg


def main():
    p = argparse.ArgumentParser(description="dump ferry 队列报文")
    p.add_argument("kind", nargs="?", choices=["req", "resp"], help="队列类型")
    p.add_argument("name", nargs="?", help="service 名或 instance_id")
    p.add_argument("--raw-key", help="直接指定 Redis key")
    p.add_argument("-n", type=int, default=10, help="最多打印几条(默认 10)")
    args = p.parse_args()

    if args.raw_key:
        key = args.raw_key
    elif args.kind and args.name:
        key = f"bridge:{args.kind}:{args.name}"
    else:
        p.error("需要 `<kind> <name>` 或 --raw-key")

    items = redis_lrange(key, args.n)
    if not items:
        print(f"{key} 为空")
        return

    print(f"{key}:{len(items)} 条\n")
    for i, item in enumerate(items):
        try:
            msg = render(json.loads(item))
        except json.JSONDecodeError as e:
            print(f"--- #{i} 无法解析为 JSON: {e} ---\n{item[:200]}\n")
            continue
        print(f"--- #{i} ---")
        print(json.dumps(msg, indent=2, ensure_ascii=False))
        print()


if __name__ == "__main__":
    main()
