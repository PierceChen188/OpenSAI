"""
OpenSAI — Distributed Computation Offloading Client.

Provides the ``opensai_client`` SDK, CLI entry point, and AgentSkill plugin
for OpenClaw-based AI agents.
"""

from setuptools import setup, find_packages

setup(
    name='opensai-client',
    version='0.1.0',
    description='OpenSAI distributed computation offloading client SDK and CLI',
    long_description=__doc__,
    author='OpenSAI Lab',
    license='MIT',
    packages=find_packages(include=['opensai_client', 'opensai_client.*',
                                     'schema', 'schema.*',
                                     'skills', 'skills.*']),
    python_requires='>=3.10',
    install_requires=[
        # No required third-party dependencies (stdlib only)
    ],
    extras_require={
        'redis': ['redis>=4.0'],
    },
    entry_points={
        'console_scripts': [
            'opensai = opensai_client.cli:main',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
