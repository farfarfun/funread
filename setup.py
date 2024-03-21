from funpypi import setup


install_requires = ["tqdm", "requests", "dominate"]


setup(
    name="funread",
    install_requires=install_requires,
    include_package_data=True,
    package_data={"": ["*.db"]},
)
