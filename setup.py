from setuptools import setup

setup(
	name="Arbitrage Bot",
    version="0.0.1",
    description="Arbitrage bot is a tool that watches cryptocurrency markets to find arbitrage opportunities",
	author="Jaume Aloy",
    author_email="jaumealoy@protonmail.com",
	install_requires=[
		"rich",
		"websocket-client",
		"requests"
    ]
)