import source_page


class FakeTable:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def upload_attachment(self, record_id, field, path):
        if self.fail:
            raise RuntimeError("boom")
        self.calls.append((record_id, field, path))


def test_finds_and_attaches(tmp_path):
    (tmp_path / "COI_forms_cert11.pdf").write_bytes(b"%PDF")
    t = FakeTable()
    assert source_page.attach_source_page(t, "rec1", "COI_forms_cert11.json", [tmp_path])
    assert t.calls == [("rec1", "Source Page", str(tmp_path / "COI_forms_cert11.pdf"))]


def test_missing_file_is_not_fatal(tmp_path):
    t = FakeTable()
    assert source_page.attach_source_page(t, "rec1", "nope.json", [tmp_path]) is False
    assert t.calls == []


def test_upload_error_is_not_fatal(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"%PDF")
    assert source_page.attach_source_page(FakeTable(fail=True), "rec1", "a.json", [tmp_path]) is False
