# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

PKG = 'Liquirizia.EventBroker.Implements.RabbitMQ'
SRC = 'src'
EXCLUDES = []
DESC = 'Event Broker Implementation of Liquirizia for RabbitMQ'
WHO = 'Heo Yongseon'

PKGS = [PKG]
DIRS = {PKG: SRC}
for package in find_packages(SRC, exclude=EXCLUDES):
	PKGS.append('{}.{}'.format(PKG, package))
	DIRS['{}.{}'.format(PKG, package)] = '{}/{}'.format(SRC, package.replace('.', '/'))

setup(
	name=PKG,
	description=DESC,
	long_description=open('README.md', encoding='utf-8').read(),
	long_description_content_type='text/markdown',
	author=WHO,
	version=open('VERSION', encoding='utf-8').read(),
	packages=PKGS,
	package_dir=DIRS,
	include_package_data=False,
	classifiers=[
		'Programming Language :: Python :: 3.8',
		'Programming Language :: Python :: 3.9',
		'Programming Language :: Python :: 3.10',
		'Programming Language :: Python :: 3.11',
		'Programming Language :: Python :: 3.12',
		'Programming Language :: Python :: 3.13',
		'Liquirizia',
		'Liquirizia :: EventBroker',
		'Liquirizia :: EventBroker :: RabbitMQ',
	],
	install_requires=[
		'Liquirizia.EventBroker@git+https://github.com/yong5eon/Liquirizia.EventBroker.git', # TODO : change PyPI
		'pika>=1.3.2'
	],
	python_requires='>=3.8'
)
