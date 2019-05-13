import tempfile


def test_db_init(runner, monkeypatch):
    class Recorder:
        called = False

    def fake_init_db():
        Recorder.called = True
    monkeypatch.setattr('tdmq.db.init_db', fake_init_db)
    result = runner.invoke(args=['db', 'init'])
    assert 'Initialized' in result.output
    assert Recorder.called


def test_db_load(runner, monkeypatch):
    class Recorder:
        called = False
        result = {'sensor_types': 2, 'nodes': 4, 'sensors': 10}
    
    def fake_load_file(fname):
        Recorder.called = fname
        return Recorder.result
    monkeypatch.setattr('tdmq.db.load_file', fake_load_file)
    with tempfile.NamedTemporaryFile() as tmpfile:
        result = runner.invoke(args=['db', 'load', tmpfile.name])
        assert Recorder.called == tmpfile.name
    assert str(Recorder.result) in result.output

