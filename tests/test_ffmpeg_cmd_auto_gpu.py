from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from shrink_media.classify import MediaInfo
from shrink_media.ffmpeg_cmd import build_video_candidates


class TestFfmpegCmdAutoGpu(unittest.TestCase):
    def _make_info(self) -> MediaInfo:
        return MediaInfo(
            kind="video",
            streams=[
                {"codec_type": "video", "codec_name": "h264", "pix_fmt": "yuv420p"},
                {"codec_type": "audio", "codec_name": "aac", "channels": 2},
            ],
            fmt={},
        )

    def test_auto_gpu_prefers_nvenc_and_does_not_fallback_to_cpu(self) -> None:
        info = self._make_info()
        out = Path("out.mp4")

        def fake_has_encoder(enc: str) -> bool:
            return enc in {"hevc_nvenc", "libx265", "libx264"}

        with patch("shrink_media.ffmpeg_cmd.has_encoder", side_effect=fake_has_encoder):
            cands = build_video_candidates(
                "in.mp4",
                out,
                info,
                container_pref="mp4",
                video_policy="transcode",
                audio_policy="transcode",
                allow_opus_in_mp4=False,
                video_encoder="auto_gpu",
                video_crf=23,
                video_preset="medium",
                pix_fmt="yuv420p",
                faststart=True,
            )

        self.assertTrue(cands)
        for cmd in cands:
            self.assertIn("-c:v", cmd)
            self.assertEqual(cmd[cmd.index("-c:v") + 1], "hevc_nvenc")
            self.assertNotIn("libx265", cmd)
            self.assertNotIn("libx264", cmd)

    def test_auto_gpu_falls_back_to_cpu_when_nvenc_unavailable(self) -> None:
        info = self._make_info()
        out = Path("out.mp4")

        def fake_has_encoder(enc: str) -> bool:
            return enc in {"libx265", "libx264"}

        with patch("shrink_media.ffmpeg_cmd.has_encoder", side_effect=fake_has_encoder):
            cands = build_video_candidates(
                "in.mp4",
                out,
                info,
                container_pref="mp4",
                video_policy="transcode",
                audio_policy="transcode",
                allow_opus_in_mp4=False,
                video_encoder="auto_gpu",
                video_crf=23,
                video_preset="medium",
                pix_fmt="yuv420p",
                faststart=True,
            )

        self.assertTrue(cands)
        joined = "\n".join(" ".join(cmd) for cmd in cands)
        self.assertIn("libx265", joined)
        self.assertNotIn("hevc_nvenc", joined)

