from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from shrink_media.classify import MediaInfo
from shrink_media.ffmpeg_cmd import build_video_candidates


class TestFfmpegCmdTolerateCorrupt(unittest.TestCase):
    def _make_info(self) -> MediaInfo:
        return MediaInfo(
            kind="video",
            streams=[
                {"codec_type": "video", "codec_name": "h264", "pix_fmt": "yuv420p"},
                {"codec_type": "audio", "codec_name": "aac", "channels": 2},
            ],
            fmt={},
        )

    def test_tolerate_corrupt_adds_error_tolerance_flags(self) -> None:
        info = self._make_info()
        out = Path("out.mp4")

        with patch("shrink_media.ffmpeg_cmd.has_encoder", return_value=False):
            cands = build_video_candidates(
                "in.mp4",
                out,
                info,
                container_pref="mp4",
                video_policy="transcode",
                audio_policy="transcode",
                allow_opus_in_mp4=False,
                video_encoder="libx265",
                video_crf=23,
                video_preset="medium",
                pix_fmt="yuv420p",
                faststart=True,
                tolerate_corrupt=True,
            )

        self.assertTrue(cands)
        cmd = cands[0]
        joined = " ".join(cmd)
        self.assertIn("-fflags +discardcorrupt", joined)
        self.assertIn("-err_detect ignore_err", joined)
        self.assertIn("-max_error_rate 1.0", joined)

    def test_default_does_not_add_tolerance_flags(self) -> None:
        info = self._make_info()
        out = Path("out.mp4")

        with patch("shrink_media.ffmpeg_cmd.has_encoder", return_value=False):
            cands = build_video_candidates(
                "in.mp4",
                out,
                info,
                container_pref="mp4",
                video_policy="transcode",
                audio_policy="transcode",
                allow_opus_in_mp4=False,
                video_encoder="libx265",
                video_crf=23,
                video_preset="medium",
                pix_fmt="yuv420p",
                faststart=True,
            )

        self.assertTrue(cands)
        cmd = cands[0]
        joined = " ".join(cmd)
        self.assertNotIn("-fflags +discardcorrupt", joined)
        self.assertNotIn("-err_detect ignore_err", joined)
        self.assertNotIn("-max_error_rate 1.0", joined)

