Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

## Tabla de Contenidos
1. [Protocolo de Comunicación](#protocolo-de-comunicación)
    * [Formato de Paquete](#formato-de-paquete)
    * [Tipo de Paquetes](#tipo-de-paquetes)
    * [Flujo de Comunicación](#flujo-de-comunicación)
2. [Mecanismos de Sincronización](#mecanismos-de-sincronización)

---

## Protocolo de Comunicación

### Formato de Paquete

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Opcode     |    Batches    |            Length             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Agency ID   |                                               |
+-+-+-+-+-+-+-+-+                                               +
|                                                               |
+                                                               +
|                    Payload (0 a 65535 bytes)                  |
+                                                               +
|                             ...                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Campo | Tamaño | Descripción |
| ----- | ------ | ----------- |
| **opcode** | 1 byte | Tipo de paquete (`0 = EOF`, `1 = ACK`, `2 = DATA`). |
| **batches** | 1 byte | Cantidad de apuestas enviadas en el paquete. |
| **payload length** | 2 bytes | Longitud del payload. |
| **agency ID** | 1 byte | Identificador de la agencia que envía el mensaje. |
| **payload** | 0 a 65535 bytes | Contenido del paquete. |

### Tipo de paquetes

Existen 3 tipos de paquetes en el protocolo definido, que se indican según el `opcode` del header del paquete.
* `2 = DATA` - Permite al emisor enviar datos al receptor por el `payload` del paquete. 
* `1 = ACK` - Permite al receptor confirmar la recepción y el correcto procesamiento del paquete previamente enviado.
* `0 = EOF` - Permite al emisor indicarle al receptor que no tiene mas datos para enviarle.

### Flujo de Comunicación

El cliente comienza leyendo el archivo indicado en `INPUT_FILE` fila a fila, y las va agrupando, separandolas por un `\n`, hasta llegar a el `BATCH_SIZE` indicado.

> [!NOTE]
> En caso de haber menos filas que `BATCH_SIZE`, se envian las que se lograron agrupar.

Una vez se tiene el batch armado, el cliente envia al servidor dichos datos y espera a recibir una confirmación, la cual le indica al cliente que el servidor pudo procesar los datos enviados de forma correcta.

Ese procesamiento del servidor consiste en obtener los datos, parsearlos para validar que sean correctos y guardarlos en un archivo. Si todo eso se realiza sin errores, se envia la confirmación al cliente.

* Si el cliente recibe la confirmación, continua enviando datos.
* Si el cliente NO recibe la confirmación, entonces el servidor tuvo un problema y aborta el programa.

> [!NOTE]
> Se aborta el programa, porque al usar TCP, el error que pudo haber ocurrido esta mas alla de un simple reintento.

Cuando el cliente termine de enviar todos los datos, le indica al servidor que ya no tiene mas datos y pasa a esperar a recibir los ganadores.

El servidor, una vez recibe que el cliente no tiene mas datos, determina los ganadores, se los envia al cliente, y espera a recibir la confirmación para cada uno de ellos (envia, espera, envia, espera, ...).

Del lado del cliente, se van recibiendo los datos y se almacenan en el `OUTPUT_FILE` indicado. Respondiendo al servidor con la confirmación si no hubo problemas.

Finalmente, cuando el servidor no tenga mas ganadores para enviar, le indica al cliente que no tiene mas datos, cierra la conexión y libera los recursos.

Entonces, cuando el cliente reciba que el servidor ya no tiene mas datos, simplemente libera los recursos utilizados y finaliza el programa.