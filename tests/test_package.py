from __future__ import annotations

import importlib.metadata

import turboblast as m


def test_version():
    assert importlib.metadata.version("turboblast") == m.__version__
