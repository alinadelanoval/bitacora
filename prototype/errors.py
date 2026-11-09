"""Errores del dominio de Bitácora, cada uno con su código del contrato.

El gateway captura `BitacoraError` y envía `code` en el campo `error` de la
respuesta gRPC. Los valores coinciden con `enum ErrorCode` de
`proto/bitacora/v1/bitacora.proto`; se copian aquí para que este módulo no
dependa del código generado.

Cada constructor pasa todos sus argumentos a `super().__init__`. Ray envía las
excepciones entre procesos con pickle, que las reconstruye llamando a la clase
con `args`; si falta algún argumento, el llamante recibe
`UnserializableException` en lugar del error.
"""


class BitacoraError(Exception):
    """Base de los errores que el gateway traduce a un ErrorCode."""

    # Código del contrato; cada subclase define el suyo.
    code: int | None = None


class CorruptBatch(BitacoraError):
    """Lote dañado: magic distinto de 2, longitud incoherente o CRC incorrecto."""

    code = 10  # ERROR_CODE_CORRUPT_MESSAGE

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class InvalidBatch(BitacoraError):
    """Lote bien formado que incumple una regla del contrato."""

    code = 18  # ERROR_CODE_INVALID_REQUEST

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class OffsetOutOfRange(BitacoraError):
    """Lectura con un offset fuera de [0, LEO]."""

    code = 4  # ERROR_CODE_OFFSET_OUT_OF_RANGE

    def __init__(self, offset: int, log_end_offset: int):
        super().__init__(offset, log_end_offset)
        self.offset = offset
        self.log_end_offset = log_end_offset

    def __str__(self) -> str:
        return f"offset {self.offset} fuera de [0, {self.log_end_offset}]"


class UnknownTopicOrPartition(BitacoraError):
    """El tema o la partición no existen."""

    code = 14  # ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION

    def __init__(self, topic: str, partition: int | None = None):
        super().__init__(topic, partition)
        self.topic = topic
        self.partition = partition

    def __str__(self) -> str:
        if self.partition is None:
            return f"tema desconocido: {self.topic}"
        return f"partición desconocida: {self.topic}-{self.partition}"


class TopicAlreadyExists(BitacoraError):
    """El tema ya existe con otros parámetros."""

    code = 17  # ERROR_CODE_TOPIC_ALREADY_EXISTS

    def __init__(self, topic: str):
        super().__init__(topic)
        self.topic = topic


class InvalidReplicationFactor(BitacoraError):
    """Factor de replicación fuera del rango admitido."""

    code = 13  # ERROR_CODE_INVALID_REPLICATION_FACTOR

    def __init__(self, replication_factor: int):
        super().__init__(replication_factor)
        self.replication_factor = replication_factor


class StorageFailure(BitacoraError):
    """La réplica no puede escribir en su archivo.

    El contrato todavía no define un código para los fallos de disco, así que
    `code` vale None y el gateway debe tratarlo como un error del servidor.
    """

    code = None

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason
