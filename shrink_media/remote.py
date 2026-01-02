# -*- coding: utf-8 -*-
"""Remote client initialization and helpers."""
from __future__ import annotations

import argparse
import sys
from typing import Optional, Tuple

from .logging import log_err
from .openlist_client import OpenListClientSync, parse_remote_location
from .utils import is_url

__all__ = [
    "get_device_id",
    "init_remote_client",
    "validate_same_server",
]


def get_device_id(args: argparse.Namespace) -> str:
    """获取设备标识符，用于多设备协同。"""
    if args.device_id:
        return args.device_id
    import os
    import platform
    try:
        return os.uname().nodename  # type: ignore[attr-defined]
    except AttributeError:
        return platform.node() or os.environ.get("COMPUTERNAME") or "unknown"


def init_remote_client(args: argparse.Namespace) -> Tuple[Optional[OpenListClientSync], Optional[str]]:
    """
    如果 input 或 output 是远端 URL，则初始化 OpenListClientSync。

    返回: (client, remote_base)
    - client: OpenListClientSync 实例，或 None（纯本地操作）
    - remote_base: 服务器基础 URL，或 None
    """
    if not is_url(args.input) and not (args.output and is_url(args.output)):
        return None, None

    base_candidate = args.input if is_url(args.input) else args.output
    remote_base, _ = parse_remote_location(base_candidate)

    if not args.openlist_user or not args.openlist_pass:
        log_err("ERROR: 远端操作需要 --openlist-user 与 --openlist-pass。")
        sys.exit(2)

    client = OpenListClientSync(
        remote_base,
        args.openlist_user,
        args.openlist_pass,
        timeout=args.openlist_timeout,
        otp_key=args.openlist_otp,
        retries=args.retries,
        retry_backoff=args.retry_backoff,
    )
    return client, remote_base


def validate_same_server(args: argparse.Namespace) -> None:
    """验证 input 和 output 在同一服务器（目前仅支持单服务器）。"""
    if is_url(args.input) and args.output and is_url(args.output):
        base_in, _ = parse_remote_location(args.input)
        base_out, _ = parse_remote_location(args.output)
        if base_in != base_out:
            log_err("ERROR: 当前版本仅支持同一 OpenList 服务器的输入/输出。")
            sys.exit(2)
