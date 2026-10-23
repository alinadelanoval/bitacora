# Scripts de desarrollo

El script [`cluster.py`](cluster.py) gestiona los tres procesos de prueba de
BIT-01.

Desde la raíz del repositorio:

```sh
make cluster
make stop
```

Requiere Linux, Python 3, Go 1.26.x y GNU Make. Los procesos y sus logs se
guardan bajo `data/dev/cluster/`, ruta ignorada por Git. El comando `stop`
valida la identidad de cada proceso antes de señalizarlo; no termina procesos
por nombre ni elimina sus datos.
