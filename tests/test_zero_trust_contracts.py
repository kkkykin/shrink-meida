from __future__ import annotations

import unittest


@unittest.skip("TODO.md: implement shrink_media_server/shrink_media_worker and enable these tests")
class TestZeroTrustContracts(unittest.TestCase):
    def test_worker_cannot_upload_outside_staging(self) -> None:
        """
        TODO.md: Worker 只能上传到：
          route.out_root/.shrink_media_staging/<task_id>/<nonce>/blob
        """
        raise NotImplementedError

    def test_finalize_is_idempotent(self) -> None:
        """
        TODO.md: finalize/complete/fail 必须是幂等接口（重复调用不制造脏状态）。
        """
        raise NotImplementedError

    def test_lease_expires_and_can_be_reassigned(self) -> None:
        """
        TODO.md: heartbeat 续租；超时自动回收重派。
        """
        raise NotImplementedError

