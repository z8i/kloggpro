import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kloggpro", 
    version="0.0.2",
    author="Martin (z8i)",
    author_email="github@z8i.de",
    description="Package for TFA KlimaLoggPro-Driver to use with Home Assistant",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/z8i/kloggpro",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pyusb'
    ],
)
