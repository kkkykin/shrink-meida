from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from shrink_media.classify import MediaInfo
from shrink_media.ffmpeg_cmd import build_video_candidates


class TestFfmpegCmdVideoTag(unittest.TestCase):
    def _make_info(self) -> MediaInfo:
        return MediaInfo(
            kind="video",
            streams=[
                {"codec_type": "video", "codec_name": "h264", "pix_fmt": "yuv420p"},
                {"codec_type": "audio", "codec_name": "aac", "channels": 2},
            ],
            fmt={},
        )

    def test_mp4_libx264_does_not_force_hvc1(self) -> None:
        info = self._make_info()
        out = Path("out.mp4")
        # build_video_candidates only needs has_encoder for selecting fallbacks; for libx264 fixed encoder
        # we can safely return False for all queries.
        with patch("shrink_media.ffmpeg_cmd.has_encoder", return_value=False):
            cands = build_video_candidates(
                "in.mkv",
                out,
                info,
                container_pref="mp4",
                video_policy="transcode",
                audio_policy="transcode",
                allow_opus_in_mp4=False,
                video_encoder="libx264",
                video_crf=23,
                video_preset="medium",
                pix_fmt="yuv420p",
                faststart=True,
            )
        self.assertTrue(cands)
        cmd = cands[0]
        self.assertFalse(any(cmd[i] == "-tag:v" and cmd[i + 1] == "hvc1" for i in range(len(cmd) - 1)))

    def test_mp4_libx265_forces_hvc1(self) -> None:
        info = self._make_info()
        out = Path("out.mp4")
        with patch("shrink_media.ffmpeg_cmd.has_encoder", return_value=False):
            cands = build_video_candidates(
                "in.mkv",
                out,
                info,
                container_pref="mp4",
                video_policy="transcode",
                audio_policy="transcode",
                allow_opus_in_mp4=False,
                video_encoder="libx265",
                video_crf=28,
                video_preset="medium",
                pix_fmt="yuv420p",
                faststart=True,
            )
        self.assertTrue(cands)
        cmd = cands[0]
        self.assertTrue(any(cmd[i] == "-tag:v" and cmd[i + 1] == "hvc1" for i in range(len(cmd) - 1)))

