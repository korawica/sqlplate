from src import get_env


def test_get_env(test_path):
    env = get_env(test_path.parent / 'templates')
    template = env.get_template('sqlite/select.sql')
    assert template.render() == "SELECT 'HELLO WORLD' AS GREETING"
