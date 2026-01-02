# -*- coding: utf-8 -*-
"""Path planning and output location logic."""
from __future__ import annotations

import argparse
import posixpath
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Tuple

from .logging import log, log_err
from .openlist_client import OpenListClientSync, parse_remote_location, remote_join
from .utils import is_url
from .workitem import WorkItem, iter_local_inputs, iter_remote_inputs

__all__ = [
    "InputPlan",
    "OutputPlan",
    "plan_input",
    "plan_output",
]


@dataclass
class InputPlan:
    """输入路径规划结果。"""
    is_remote: bool
    root_local: Optional[Path]
    root_remote_path: Optional[str]
    display: str
    items_iter: Iterator[WorkItem]


@dataclass
class OutputPlan:
    """输出路径规划结果。"""
    is_remote: bool
    root_local: Optional[Path]
    root_remote_path: Optional[str]
    display: str


def plan_input(
    args: argparse.Namespace,
    remote_client: Optional[OpenListClientSync],
) -> InputPlan:
    """
    规划输入路径，返回 InputPlan。

    根据 args.input 是本地路径还是远端 URL，分别处理。
    """
    input_is_remote = is_url(args.input)

    if input_is_remote:
        if remote_client is None:
            log_err("ERROR: OpenList 输入需要远端客户端。")
            sys.exit(2)
        in_base, in_root_remote_path = parse_remote_location(args.input)
        in_root_remote_path, items_iter = iter_remote_inputs(
            remote_client,
            in_root_remote_path,
            image_codec=args.image_codec,
            container=args.container,
            audio_policy=args.audio_policy,
            out_name_mode=args.out_name_mode,
        )
        return InputPlan(
            is_remote=True,
            root_local=None,
            root_remote_path=in_root_remote_path,
            display=f"{in_base}{in_root_remote_path}",
            items_iter=items_iter,
        )

    in_path = Path(args.input).expanduser().resolve()
    if not in_path.exists():
        log_err("ERROR: 输入路径不存在。")
        sys.exit(2)

    in_root_local, items_iter = iter_local_inputs(
        in_path,
        image_codec=args.image_codec,
        container=args.container,
        audio_policy=args.audio_policy,
        out_name_mode=args.out_name_mode,
    )
    return InputPlan(
        is_remote=False,
        root_local=in_root_local,
        root_remote_path=None,
        display=str(in_root_local),
        items_iter=items_iter,
    )


def plan_output(
    args: argparse.Namespace,
    input_plan: InputPlan,
    remote_client: Optional[OpenListClientSync],
    remote_base: Optional[str],
) -> Tuple[OutputPlan, Optional[OpenListClientSync], Optional[str]]:
    """
    规划输出路径，返回 (OutputPlan, updated_remote_client, updated_remote_base)。

    根据 --inplace、--output、input 类型等情况推导输出位置。
    可能需要创建新的 remote_client（当 output 是远端但 input 是本地时）。
    """
    if args.inplace:
        if input_plan.is_remote:
            return OutputPlan(
                is_remote=True,
                root_local=None,
                root_remote_path=input_plan.root_remote_path,
                display=f"{remote_base or ''}{input_plan.root_remote_path or ''}",
            ), remote_client, remote_base
        return OutputPlan(
            is_remote=False,
            root_local=input_plan.root_local,
            root_remote_path=None,
            display=str(input_plan.root_local),
        ), remote_client, remote_base

    if args.output is None:
        if input_plan.is_remote:
            assert input_plan.root_remote_path is not None
            path = input_plan.root_remote_path.rstrip("/")
            parent, name = posixpath.split(path)
            if name == "":
                name = "__compressed"
            else:
                name = name + "__compressed"
            new_path = f"{parent}/{name}" if parent else f"/{name}"
            return OutputPlan(
                is_remote=True,
                root_local=None,
                root_remote_path=new_path,
                display=f"{remote_base or ''}{new_path}",
            ), remote_client, remote_base
        assert input_plan.root_local is not None
        out_root_local = input_plan.root_local.parent / (input_plan.root_local.name + "__compressed")
        out_root_local.mkdir(parents=True, exist_ok=True)
        return OutputPlan(
            is_remote=False,
            root_local=out_root_local,
            root_remote_path=None,
            display=str(out_root_local),
        ), remote_client, remote_base

    if is_url(args.output):
        base_out, path_out = parse_remote_location(args.output)
        if remote_client is None:
            remote_base = base_out
            remote_client = OpenListClientSync(
                base_out,
                args.openlist_user,
                args.openlist_pass,
                timeout=args.openlist_timeout,
                otp_key=args.openlist_otp,
                retries=args.retries,
                retry_backoff=args.retry_backoff,
            )
        out_root_remote_path = path_out if path_out.endswith("/") else path_out + "/"
        return OutputPlan(
            is_remote=True,
            root_local=None,
            root_remote_path=out_root_remote_path,
            display=f"{base_out}{out_root_remote_path}",
        ), remote_client, remote_base

    out_root_local = Path(args.output).expanduser().resolve()
    out_root_local.mkdir(parents=True, exist_ok=True)
    return OutputPlan(
        is_remote=False,
        root_local=out_root_local,
        root_remote_path=None,
        display=str(out_root_local),
    ), remote_client, remote_base
