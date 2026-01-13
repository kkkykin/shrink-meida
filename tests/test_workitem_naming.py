from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from shrink_media.workitem import (
    apply_out_name_mode,
    build_suffixed_target_name,
    compute_image_out_name_overrides,
    compute_output_name_overrides,
    iter_local_inputs,
    make_default_out_name_of,
    normalize_ext,
)


class TestWorkitemNaming(unittest.TestCase):
    def test_normalize_ext(self) -> None:
        self.assertEqual(normalize_ext(""), "")
        self.assertEqual(normalize_ext("mp4"), ".mp4")
        self.assertEqual(normalize_ext(".MP4"), ".mp4")

    def test_build_suffixed_target_name(self) -> None:
        self.assertEqual(build_suffixed_target_name("a.mp4", target_ext=".mkv"), "a__mp4.mkv")
        self.assertEqual(build_suffixed_target_name("a.MP4", target_ext="mkv"), "a__mp4.mkv")
        self.assertEqual(build_suffixed_target_name("a", target_ext=".mkv"), "a__src.mkv")

    def test_apply_out_name_mode_suffix(self) -> None:
        p = Path("/out/a.mp4")
        self.assertEqual(apply_out_name_mode(p, src_rel="a.mov", target_ext=".mp4", out_name_mode="suffix"), Path("/out/a__mov.mp4"))
        self.assertEqual(apply_out_name_mode(p, src_rel="a.mp4", target_ext=".mp4", out_name_mode="suffix"), p)
        self.assertEqual(apply_out_name_mode(p, src_rel="a.mov", target_ext="", out_name_mode="suffix"), p)
        self.assertEqual(apply_out_name_mode(p, src_rel="a.mov", target_ext=".mp4", out_name_mode="collision"), p)

    def test_compute_output_name_overrides_prefers_existing_target(self) -> None:
        names = ["1.webp", "1.jpg"]
        out_name_of = make_default_out_name_of(
            image_target_ext=".webp",
            audio_target_ext="",
            video_target_ext_by_name={},
            out_name_mode="collision",
        )
        overrides = compute_output_name_overrides(names, out_name_of=out_name_of)
        self.assertEqual(overrides, {"1.jpg": "1__jpg.webp"})

    def test_compute_output_name_overrides_avoids_reserved_names(self) -> None:
        names = ["1.webp", "1.jpg", "1__jpg.webp"]
        out_name_of = make_default_out_name_of(
            image_target_ext=".webp",
            audio_target_ext="",
            video_target_ext_by_name={},
            out_name_mode="collision",
        )
        overrides = compute_output_name_overrides(names, out_name_of=out_name_of)
        self.assertEqual(overrides, {"1.jpg": "1__jpg__2.webp"})

    def test_compute_image_out_name_overrides_collision_renames_all(self) -> None:
        names = ["1.jpg", "1.png"]
        overrides = compute_image_out_name_overrides(names, image_target_ext=".webp", out_name_mode="collision")
        self.assertEqual(overrides, {"1.jpg": "1__jpg.webp", "1.png": "1__png.webp"})

    def test_iter_local_inputs_emits_out_rel_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "1.jpg").write_bytes(b"")
            (root / "1.png").write_bytes(b"")

            _root, items_iter = iter_local_inputs(root, image_codec="webp", out_name_mode="collision")
            items = list(items_iter)
            by_rel = {it.rel: it for it in items}
            self.assertIn("1.jpg", by_rel)
            self.assertIn("1.png", by_rel)
            self.assertIsNone(by_rel["1.jpg"].out_rel_override)
            self.assertEqual(by_rel["1.png"].out_rel_override, "1__png.webp")

