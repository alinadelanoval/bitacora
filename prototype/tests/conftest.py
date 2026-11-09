"""Preparación compartida por las pruebas del prototipo."""

import uuid

import pytest
import ray

from prototype.config import NAMESPACE, Config

# Un nodo Ray con las tres etiquetas de nodo lógico.
NODE_RESOURCES = {"node:n1": 1, "node:n2": 1, "node:n3": 1}


@pytest.fixture(scope="session")
def ray_address():
    """Arranca Ray una vez para todas las pruebas que lo necesitan."""
    ray.init(
        num_cpus=2,
        resources=NODE_RESOURCES,
        namespace=NAMESPACE,
        include_dashboard=False,
        log_to_driver=False,
    )
    yield ray.get_runtime_context().gcs_address
    ray.shutdown()


@pytest.fixture
def config(tmp_path):
    """Carpeta de datos propia para cada prueba."""
    return Config(data_dir=tmp_path / "data")


@pytest.fixture
def topic():
    """Nombre de tema único, para que los actores de una prueba no se crucen con los de otra."""
    return f"t{uuid.uuid4().hex[:8]}"


@pytest.fixture
def detached_actors(ray_address):
    """Lista de nombres de actores detached que la prueba crea; se eliminan al terminar."""
    names: list[str] = []
    yield names
    for name in names:
        try:
            ray.kill(ray.get_actor(name, namespace=NAMESPACE))
        except ValueError:
            pass
