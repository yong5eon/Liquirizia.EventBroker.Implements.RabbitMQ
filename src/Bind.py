# -*- coding: utf-8 -*-

from collections.abc import Mapping
from operator import eq, ne
from typing import Any, Iterator

__all__ = (
	'Parameters',
	'All',
	'Any',
)


class Parameters(Mapping):
	def __init__(self, **kwargs):
		self.args = kwargs
		return
	
	def __iter__(self) -> Iterator:
		return self.args.__iter__()
	
	def __reversed__(self) -> Iterator:
		return self.args.__reversed__()
	
	def __len__(self) -> int:
		return self.args.__len__()
	
	def __getitem__(self, key: Any) -> Any:
		return self.args.__getitem__(key)
	
	def __contains__(self, key: object) -> bool:
		return self.args.__contains__(key)
	
	def __eq__(self, other) -> bool:
		return eq(self.args, other)
	
	def __ne__(self, other) -> bool:
		return ne(self.args, other)
	
	def keys(self) -> Iterator:
		return self.args.keys()
	
	def items(self):
		return self.args.items()
	
	def values(self):
		return self.args.values()


class All(Parameters):
	def __init__(self, **kwargs):
		headers = {
			'x-match': 'all-with-x'
		}
		headers.update(kwargs)
		super().__init__(**headers)
		return


class Any(Parameters):
	def __init__(self, **kwargs):
		headers = {
			'x-match': 'any-with-x'
		}
		headers.update(kwargs)
		super().__init__(**headers)
		return