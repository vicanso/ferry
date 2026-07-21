#!/usr/bin/env python3
"""把 ferry 队列里的报文打印成人类可读的形式,body 自动解 base64。

线上格式用 base64 是为了能原样承载任意字节(gzip 响应、图片、protobuf),
可读性由这个脚本补回来,不需要牺牲协议的透明性。

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


def decode_body(b64):
    """返回 (展示用文本, 说明)。二进制 body 不强行解码,给出十六进制预览。"""
    if not b64:
        return "", "空"
    raw = base64.b64decode(b64)
    try:
        return raw.decode("utf-8"), f"{len(raw)} 字节 UTF-8"
    except UnicodeDecodeError:
        preview = " ".join(f"{b:02x}" for b in raw[:16])
        hint = " (gzip)" if raw[:2] == b"\x1f\x8b" else ""
        return f"<二进制 {preview}...>", f"{len(raw)} 字节二进制{hint}"


def render(msg):
    """就地把 body 字段换成可读形式,并附一行尺寸说明。"""
    if "body" in msg:
        msg["body"], note = decode_body(msg["body"])
        msg["_body_info"] = note
    result = msg.get("result")
    if isinstance(result, dict):
        for branch in ("Ok", "Err"):
            inner = result.get(branch)
            if isinstance(inner, dict) and "body" in inner:
                inner["body"], note = decode_body(inner["body"])
                inner["_body_info"] = note
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
