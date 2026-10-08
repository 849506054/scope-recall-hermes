"""Remote companion configuration is explicit, bounded and credential-name only."""

from dataclasses import asdict
from types import MappingProxyType, SimpleNamespace

import pytest
from scope_recall.runtime.instance import VectorRuntimeConfig, default_vector_factory
from scope_recall.runtime.worker_entry import credential_environment
from scope_recall.vector.qdrant_config import QdrantConfig


def vector_mapping(tmp_path, **qdrant):
    return dict(
        backend="qdrant",
        storage_dir=str(tmp_path / "vectors"),
        table_name="TEST_vectors",
        dimensions=2048,
        qdrant=dict(url="http://scope-recall-qdrant:6333", **qdrant),
    )


def test_qdrant_config_roundtrips_and_factory_stays_lazy(tmp_path, monkeypatch):
    config = VectorRuntimeConfig.from_mapping(vector_mapping(tmp_path))
    assert config.qdrant.url == "http://scope-recall-qdrant:6333"
    assert config.qdrant.api_key_env == "SCOPE_RECALL_QDRANT_API_KEY"
    assert config.qdrant.collection_prefix == "scope-recall"
    assert config.qdrant.timeout_seconds == 2.0
    assert VectorRuntimeConfig.from_mapping(asdict(config)) == config
    calls = []
    monkeypatch.setattr(
        "scope_recall.runtime.instance.build_vector_store", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    default_vector_factory(config)
    assert calls[0][0] == ("qdrant",)
    assert calls[0][1]["qdrant"] == asdict(config.qdrant)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.test",
        "https://user:secret@example.test",
        "https://example.test/path",
        "https://example.test?key=secret",
        "https://example.test#fragment",
        "http://example.test:6333",
        "https://example.test:99999",
        " https://example.test",
        "https://example.test\n",
        "http://",
        "https://example.test?",
        "https://example.test#",
        "http://0x08080808:6333",
        "http://0X08080808",
        "http://0x7f000001",
        "http://134744072",
        "http://010.010.010.010",
        "http://127.1",
        "http://0x08.0x08.0x08.0x08",
        "https://0x08080808",
    ],
)
def test_qdrant_url_rejects_ambiguous_or_unprotected_routes(tmp_path, url):
    raw = vector_mapping(tmp_path)
    raw["qdrant"]["url"] = url
    with pytest.raises(ValueError, match="qdrant_url"):
        VectorRuntimeConfig.from_mapping(raw)


@pytest.mark.parametrize(
    "url",
    ["https://vectors.example.test:6333", "http://127.0.0.1:6333", "http://192.168.5.6:6333", "http://[::1]:6333"],
)
def test_qdrant_url_accepts_tls_and_internal_routes(tmp_path, url):
    raw = vector_mapping(tmp_path)
    raw["qdrant"]["url"] = url
    assert VectorRuntimeConfig.from_mapping(raw).qdrant.url == url


@pytest.mark.parametrize(
    "bad",
    [
        dict(timeout_seconds=True),
        dict(timeout_seconds=0),
        dict(timeout_seconds=float("nan")),
        dict(timeout_seconds=46),
        dict(api_key_env="secret=value"),
        dict(api_key_env=""),
        dict(collection_prefix="../collection"),
        dict(api_key="secret"),
        dict(typo=True),
    ],
)
def test_qdrant_rejects_bad_options_and_inline_secrets(tmp_path, bad):
    with pytest.raises(ValueError):
        VectorRuntimeConfig.from_mapping(vector_mapping(tmp_path, **bad))


@pytest.mark.parametrize("timeout", [0.001, 1, 45.0])
def test_qdrant_timeout_preserves_value_and_closed_bounds(timeout):
    config = QdrantConfig.from_mapping(MappingProxyType(dict(url="https://example.test", timeout_seconds=timeout)))
    assert config.timeout_seconds is timeout


@pytest.mark.parametrize("timeout", [True, "2", None, 0, 0.0009, 45.001, float("nan"), float("inf"), -float("inf")])
def test_qdrant_timeout_rejection_names_the_field(timeout):
    with pytest.raises(ValueError, match="^qdrant_timeout_seconds$"):
        QdrantConfig("https://example.test", timeout_seconds=timeout)


def test_qdrant_timeout_preserves_float_overflow():
    with pytest.raises(OverflowError):
        QdrantConfig("https://example.test", timeout_seconds=10**400)


@pytest.mark.parametrize(
    ("raw", "field"),
    [(None, "qdrant"), ([], "qdrant"), ({}, "qdrant_url"), ({"typo": True}, "qdrant_fields")],
)
def test_qdrant_mapping_rejection_names_the_field(raw, field):
    with pytest.raises(ValueError, match=f"^{field}$"):
        QdrantConfig.from_mapping(raw)


def test_backend_options_must_match_selected_backend(tmp_path):
    raw = vector_mapping(tmp_path)
    del raw["qdrant"]
    with pytest.raises(ValueError, match="qdrant"):
        VectorRuntimeConfig.from_mapping(raw)
    raw = vector_mapping(tmp_path)
    raw["backend"] = "sqlite-bruteforce"
    with pytest.raises(ValueError, match="qdrant"):
        VectorRuntimeConfig.from_mapping(raw)
    del raw["qdrant"]
    assert VectorRuntimeConfig.from_mapping(raw).qdrant is None


def test_resume_loads_only_declared_qdrant_credential(tmp_path):
    vector = VectorRuntimeConfig.from_mapping(vector_mapping(tmp_path, api_key_env="TEST_QDRANT_KEY"))
    config = SimpleNamespace(vector=vector, auxiliary=None)
    env = tmp_path / "credentials.env"
    env.write_text("TEST_QDRANT_KEY='test-fixture-key'\nUNRELATED_KEY=private\n")
    assert credential_environment(config, env) == {"TEST_QDRANT_KEY": "test-fixture-key"}
    legacy = SimpleNamespace(vector=None, auxiliary=None)
    assert credential_environment(legacy, env) == {}


@pytest.mark.parametrize("url", ["http://0x08080808:6333", "http://134744072:6333", "http://0177.0.0.1.:6333"])
def test_alternative_numeric_hosts_are_not_trusted_as_internal(tmp_path, url):
    """A C resolver reads these as addresses; the origin rule refuses them instead."""
    raw = vector_mapping(tmp_path)
    raw["qdrant"]["url"] = url
    with pytest.raises(ValueError, match="qdrant_url"):
        VectorRuntimeConfig.from_mapping(raw)
