from jinja2 import Environment, PackageLoader


def get_env():
    return Environment(
        loader=PackageLoader(
            package_name='templates', package_path='../templates'
        ),
    )


def load_sql(env: Environment):
    template = env.get_template('databricks/select.sql')
    print(template.render(the='variables', go='here'))


if __name__ == '__main__':
    load_sql(get_env())
