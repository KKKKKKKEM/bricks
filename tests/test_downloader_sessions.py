import threading
import unittest

from bricks.downloader import AbstractDownloader
from bricks.lib.request import Request


class FakeSession:
    def __init__(self, options):
        self.options = options
        self.closed = False

    def close(self):
        self.closed = True


class FakeDownloader(AbstractDownloader):
    def make_session(self, **options):
        return FakeSession(options)

    def close_session(self, session):
        session.close()


class IsolatedDownloader(FakeDownloader):
    reuse_session_by_default = False


class DownloaderSessionTests(unittest.TestCase):
    def test_session_is_reused_per_downloader_and_thread(self):
        downloader = FakeDownloader()
        self.assertIs(downloader.get_session(), downloader.get_session())

        sessions = []
        thread = threading.Thread(
            target=lambda: sessions.append(downloader.get_session())
        )
        thread.start()
        thread.join()

        self.assertIsNot(sessions[0], downloader.get_session())

    def test_downloader_instances_do_not_share_sessions(self):
        first = FakeDownloader()
        second = FakeDownloader()

        self.assertIsNot(first.get_session(), second.get_session())

    def test_session_configuration_has_its_own_pool_entry(self):
        downloader = FakeDownloader()

        first = downloader.get_session(proxy="http://proxy-a")
        same = downloader.get_session(proxy="http://proxy-a")
        other = downloader.get_session(proxy="http://proxy-b")

        self.assertIs(first, same)
        self.assertIsNot(first, other)

    def test_clear_session_closes_and_removes_cached_sessions(self):
        downloader = FakeDownloader()
        first = downloader.get_session()
        second = downloader.get_session(proxy="http://proxy")

        downloader.clear_session()

        self.assertTrue(first.closed)
        self.assertTrue(second.closed)
        self.assertIsNot(downloader.get_session(), first)

    def test_request_uses_downloader_default_unless_explicitly_overridden(self):
        shared = FakeDownloader()
        isolated = IsolatedDownloader()

        self.assertTrue(shared.should_reuse_session(Request("https://example.com")))
        self.assertFalse(
            isolated.should_reuse_session(Request("https://example.com"))
        )
        self.assertFalse(
            shared.should_reuse_session(
                Request("https://example.com", use_session=False)
            )
        )
        self.assertTrue(
            isolated.should_reuse_session(
                Request("https://example.com", use_session=True)
            )
        )


if __name__ == "__main__":
    unittest.main()
