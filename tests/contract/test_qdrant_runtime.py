"""Trusted identity reaches a remote store independently of connection settings."""
from dataclasses import replace
import sys
from types import ModuleType

import pytest

from scope_recall.contracts import InstanceBinding
from scope_recall.runtime.auxiliary import AuxiliaryRuntimeConfig
from scope_recall.runtime.instance import RuntimeInstanceConfig, VectorRuntimeConfig, build_runtime_instance
from scope_recall.vector.qdrant_config import QdrantConfig
from scope_recall.vector.store import build_vector_store


def runtime_config(tmp_path):
    binding = InstanceBinding("TEST-agent", "TEST-install", tmp_path / "truth",
                              frozenset({"TEST-scope"}), True)
    vector = VectorRuntimeConfig("qdrant", tmp_path / "vectors", "TEST-vectors", 2,
                                 test_injection_override=True,
                                 qdrant=QdrantConfig("http://localhost:6333"))
    return RuntimeInstanceConfig(binding, "TEST-session", binding.scope_ids, vector=vector,
                                 auxiliary=AuxiliaryRuntimeConfig.from_mapping(
                                     {"external_embedding": False, "external_consolidation": False}))


def test_runtime_binds_remote_factory_without_constructing_store(tmp_path, monkeypatch):
    config = runtime_config(tmp_path)
    calls = []
    monkeypatch.setattr("scope_recall.vector.store.build_vector_store",
                        lambda *args, **kwargs: calls.append((args, kwargs)) or object())
    instance = build_runtime_instance(config)
    try:
        assert calls == []
        instance._vector_factory(config.vector)
        assert len(calls) == 1
        assert calls[0][1]["binding"] is config.binding
        assert calls[0][1]["embedding_space"] == config.embedding_space_id()
        assert not config.vector.storage_dir.exists()
    finally:
        instance.close()


def test_custom_factory_preserves_one_argument_seam(tmp_path):
    config = runtime_config(tmp_path)
    calls = []
    def custom(value):
        calls.append(value)
    instance = build_runtime_instance(config, vector_factory=custom)
    try:
        instance._vector_factory(config.vector)
        assert calls == [config.vector]
    finally:
        instance.close()


def test_store_factory_passes_validated_config_and_identity(tmp_path, monkeypatch):
    config = runtime_config(tmp_path)
    captured = []
    module = ModuleType("scope_recall.vector.qdrant_store")
    module.QdrantVectorStore = lambda *args, **kwargs: captured.append((args, kwargs)) or object()
    monkeypatch.setitem(sys.modules, module.__name__, module)
    result = build_vector_store("qdrant", storage_dir=config.vector.storage_dir,
                               table_name=config.vector.table_name, dimensions=2,
                               qdrant={"url": "http://localhost:6333"}, binding=config.binding,
                               embedding_space=config.embedding_space_id())
    assert result is not None
    args, kwargs = captured[0]
    assert args == (config.vector.storage_dir,)
    assert kwargs["binding"] is config.binding
    assert kwargs["embedding_space"] == config.embedding_space_id()
    assert isinstance(kwargs["config"], QdrantConfig)


def test_factory_rejects_unbound_remote_and_mixed_settings(tmp_path):
    options = dict(storage_dir=tmp_path, table_name="TEST", dimensions=2)
    with pytest.raises(ValueError, match="qdrant_binding"):
        build_vector_store("qdrant", **options, qdrant={"url": "http://localhost:6333"})
    with pytest.raises(ValueError, match="qdrant_options"):
        build_vector_store("sqlite-bruteforce", **options, qdrant={"url": "http://localhost:6333"})


def test_legacy_runtime_factory_preserves_original_keywords(tmp_path, monkeypatch):
    config = runtime_config(tmp_path)
    config = replace(config, vector=replace(config.vector, backend="sqlite-bruteforce", qdrant=None))
    calls = []
    monkeypatch.setattr("scope_recall.vector.store.build_vector_store",
                        lambda *args, **kwargs: calls.append((args, kwargs)))
    instance = build_runtime_instance(config)
    try:
        instance._vector_factory(config.vector)
        assert set(calls[0][1]) == {"storage_dir", "table_name", "dimensions", "metric"}
    finally:
        instance.close()
