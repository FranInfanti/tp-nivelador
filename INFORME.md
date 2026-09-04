Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

## Tabla de Contenidos
1. [Protocolo de Comunicación](#protocolo-de-comunicación)
    * [Formato de Paquete](#formato-de-paquete)
    * [Tipo de Paquetes](#tipo-de-paquetes)
    * [Flujo de Comunicación](#flujo-de-comunicación)
2. [Mecanismos de Sincronización](#mecanismos-de-sincronización)
    * [Diccionario de _threds_](#diccionario-de-threads)
    * [Acceso a _Lottery_](#acceso-a-lottery)
    * [Verificar el _quorum_](#verificar-el-quorum)

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
| **batches** | 1 byte | Cantidad de apuestas enviadas en el **payload** del paquete. |
| **length** | 2 bytes | Longitud del payload. |
| **agency ID** | 1 byte | Identificador de la agencia que envía el mensaje. |
| **payload** | 0 a 65535 bytes | Contenido del paquete. |

> [!NOTE]
> Para el diseño del formato del paquete se asumio que el contenido que se envia por el mismo es conocido por ambas partes de la conexión, pues como nos encontramos en una arquitectura de cliente-servidor, donde el servidor modela un sistema de apuestas que impone un formato de apuesta (la entidad `Bet`), por lo que los clientes deben respetar ese formato al enviar apuestas al servidor.

### Tipos de paquetes

Existen 3 tipos de paquetes en el protocolo definido, estos se indican según el `opcode` del header del paquete.
* `2 = DATA` - Informa al receptor que el emisor esta enviando datos por el `payload` del paquete. 
* `1 = ACK` - Informa al emisor que el receptor pudo procesar correctamente le paquete previamente enviado.
* `0 = EOF` - Informa al receptor que el emisor no tiene mas datos para enviarle, por lo que se avanza a la siguiente fase de la comunicación.

### Flujo de Comunicación

El cliente lee del archivo indicado en `INPUT_FILE` linea a linea, donde cada una representa una apuesta, que se las va agrupando, separandolas por un `\n`, hasta llegar al `BATCH_SIZE` indicado. En caso de haber menos filas que el `BATCH_SIZE` indicado, se envian las que se lograron agrupar.

> [!WARNING]
> Anteriormente, por cada apuesta, se reservaba 1 byte mas para indicar el `length` de la misma ([LENGTH | BET]), por lo que no se necesitaba depender de un `\n` para poder separar las apuestas. Pero se termino descartando dicha solución porque tenia mucho overhead de memoria.

El cliente, una vez que tiene el batch armado, crea el paquete y lo envia al servidor. Antes de volver a armar otro batch, el cliente espera a recibir una confirmación del servidor, de esta forma no se bombardea al servidor con datos posiblemente incorrectos.

* Si recibe la confirmación, repite el flujo anterior.
* Si NO recibe la confirmación, se aborta el programa para evitar enviar datos incorrectos al servidor.

> [!NOTE]
> Se aborta el programa, porque al usar TCP, los datos llegan al otro extremo de la conexión completos y en orden, por lo que el error esta relacionado a información incorrecta (apuestas con un formato invalido) o un error durante la ejecución interna del servidor.

¿Qué determina si el servidor confirma o no el paquete?, basicamente el servidor lo que hace es obtener la/s apuesta/s del batch, las parsea a la entidad que representa una apuesta y las persiste en un archivo. Si todo esto se realiza sin errores, entonces se confirma el paquete.

Una vez el cliente termina de enviar todas las apuestas, se lo indica al servidor con el tipo de paquete adecuado y pasa a la siguiente fase, que consiste en esperar a recibir los ganadores.

Del otro lado, el servidor una vez que recibe que el cliente no tiene mas apuestas, pasa a esperar a que se satisfaga el `AGENCY_QUORUM_MIN`. Una vez que se cumpla, obtiene todas las apuestas, filtra aquellas que corresponden al cliente con el que esta hablando y que hayan ganado, y se las envia al cliente. Nuevamente, por cada ganador enviado se espera a recibir la confirmación por parte del cliente.

* Si recibe la confirmación, continua enviando datos.
* Si NO recibe la confirmación, entonces el cliente tuvo un problema y aborta el _hilo de ejecución_.

Del lado del cliente, se van recibiendo los ganadores y se los almacena en el `OUTPUT_FILE` indicado. Respondiendo al servidor con la confirmación si no hubo problemas durante el proceso.

Finalmente, cuando el servidor no tenga mas ganadores para enviar, le indica al cliente que no tiene mas ganadores mediante el tipo de paquete adecuado, y procede a cerrar la conexión.

Entonces, cuando el cliente reciba que el servidor ya no tiene mas ganadores, simplemente libera los recursos utilizados y finaliza el programa.

## Mecanismos de Sincronización

Se implemento una solución que permite aceptar conexiones de forma concurrente mediante el uso de _multithreading_, donde por cada cliente se lanza un _thread_ que va a ser el encargado de manejar el flujo de dicha conexión.

Cada uno de los _threads_ va a tomar el rol del servidor, que fue explicado en la [sección anterior](#flujo-de-comunicación). Ahora, al introducir _multithreading_, es necesario sincronizar el accesso a los recursos compartidos para evitar los problemas clasicos de la concurrencia.

Existen un total de 3 recursos compartidos entre los _threads_ que se vayan a crear.

### Diccionario de _threads_

El _main thread_, que es el encargado de aceptar conexiones y delegar su manejo a otros _threads_, necesita mantener un registro de los _threads_ que se van creando, por lo que se define un diccionario donde se mapea un _thread_ al _socket_ de la conexión. De esta forma cuando se reciba la señal `SIGTERM` se puede proceder con el _graceful exit_. 

¿En que momento es necesario sincronizar el acceso?, como a medida que los _threads_ vayan finalizando se los debe eliminar de este diccionario, la forma mas sencilla es que él mismo se elimine cuando termine. Por lo que para evitar que haya un _thread_ añadiendo, eliminado, o leyendo, elementos del diccionario y otro añadiendo, eliminado, o leyendo, se debe sincronizar el acceso mediante el uso de _locks_.

Por lo que para poder acceder a este diccionario, es necesario tener el _lock_ que lo protege.

### Acceso a _Lottery_

_Lottery_ es una clase que internamente tiene dos métodos que acceden a un archivo, dentro de ese archivo se van a ir persistiendo las apuestas que les van llegando a cada uno de los _threads_. Y luego, se lee ese archivo para obtener todas las apuestas y determinar los ganadores.

Entonces, para evitar que un _thread_ lea/escriba del archivo y otros tambien lean/escriban del mismo, se debe sincronizar el acceso por medio de un _lock_.

Sin embargo, para evitar estar tomando y soltando el _lock_ constantemente, se implemento de tal forma que se toma una vez, se hace todo lo que se tiene que hacer y luego se suelta. Es decir se toma por el minimo tiempo necesario, para evitar que los demas _threads_ se quede esperando demasiado tiempo.

### Verificar el _quorum_

Como para poder determinar los ganadores es necesario que un minimo de agencias hayan enviado sus apuestas, de alguna forma se debe saber cuantas agencias ya enviaron sus apuestas. Luego se define una variable compartida por todos los _threads_ que se inicializa en `AGENCY_QUORUM_MIN`, cada uno de ellos, a medida que van finalizando restan en una unidad y verifican si el valor de la variable es 0, para poder proceder a determinar los ganadores.

Por un lado, es necesario definir un _lock_ sobre dicha variable para que las operaciones sobre la misma sean atomicas y no ocurran _race conditions_. Y por otro lado, para evitar el _busy wait_ lo mejor es utilizar _conditional variables_ que permiten esperar a que una condición cambie sin consumir recursos de la CPU.

Luego se define una _condvar_ asociada a esa variable, para que cuando un _thread_ tenga que verificar si se cumple la condición, primero toma el _lock_ asociado a esa variable, resta en una unidad de forma atomica, y verifica la condición.

* Si se cumple, notifica a todos los demas _threads_ que estaban esperando.
* Si NO se cumple, se va a dormir y espera a ser notificado por otro _thread_.
