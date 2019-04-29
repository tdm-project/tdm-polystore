# FIXME: We are assuming that we have access to the DB mentioned in conftest.py


def test_init_db(runner):
    result = runner.invoke(args=['db', 'init'])
    assert 'Initialized' in result.output

