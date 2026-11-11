"""Preparación compartida por las pruebas del prototipo."""

import time
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
    # Cierre ordenado de los actores que siguen vivos antes de apagar Ray.
    for actor in ray.util.list_named_actors(all_namespaces=True):
        stop_actor(actor["name"], namespace=actor["namespace"])
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
        stop_actor(name)


def stop_actor(name: str, namespace: str = NAMESPACE, timeout: float = 30.0) -> None:
    """Termina un actor con nombre de forma ordenada y espera a que muera.

    A diferencia de ``ray.kill``, ``__ray_terminate__`` deja que el proceso
    ejecute sus manejadores ``atexit``; así, cuando las pruebas se miden con
    coverage, los datos del actor se guardan antes de que termine.
    """
    try:
        handle = ray.get_actor(name, namespace=namespace)
    except ValueError:
        return  # ya no existe
    handle.__ray_terminate__.remote()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            ray.get_actor(name, namespace=namespace)
        except ValueError:
            return  # el nombre se libera cuando el actor muere
        time.sleep(0.05)
    ray.kill(handle)  # no terminó a tiempo: se fuerza
