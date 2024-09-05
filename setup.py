from os import path as os_path

from setuptools import setup, find_packages

this_directory = os_path.abspath(os_path.dirname(__file__))


# 读取文件内容
def read_file(filename):
    with open(os_path.join(this_directory, filename), encoding='utf-8') as f:
        long_description = f.read()
    return long_description


# 获取依赖
def read_requirements(filename):
    return [
        line.strip() for line in read_file(filename).splitlines()
        if not line.startswith('#')
    ]


setup(
    name='bricks-py',  # 包名
    python_requires='>=3.8.0',  # python环境
    url='https://github.com/KKKKKKKEM/bricks.git',
    long_description_content_type="text/markdown",
    long_description=open('README.md', encoding='utf-8').read(),
    version="0.0.23",  # 包的版本
    description="quickly build your crawler",  # 包简介，显示在PyPI上
    author="Kem",  # 作者相关信息
    author_email='531144129@qq.com',
    # 指定包信息，还可以用find_packages()函数
    packages=find_packages(),
    # 读取需要的数据文件
    package_data={
        'bricks': ['tpls/*', 'tpls/spider/*']
    },
    include_package_data=True,
    install_requires=read_requirements('requirements.txt'),  # 指定需要安装的依赖
    # 其他依赖版本
    extras_require={
        "mongo": ["pymongo==4.6.0"]
    },
    license="MIT",
    keywords=['bricks'],
    # script_name="setup.py",
    # script_args="sdist bdist_wheel".split(" "),

)

# python setup.py sdist bdist_wheel upload -r pypi
