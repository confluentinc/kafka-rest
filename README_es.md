# Kafka REST Proxy

<!-- hy-mt2-i18n:start -->
[English](./README.md) | [中文](./README_zh-CN.md) | [日本語](./README_ja.md) | **Español**
<!-- hy-mt2-i18n:end -->


Kafka REST Proxy ofrece una interfaz RESTful para un clúster de Kafka. Permite producir y consumir datos, ver el estado del clúster y realizar acciones administrativas de manera sencilla, sin necesidad de utilizar el protocolo o clientes nativos de Kafka. Algunos ejemplos de casos de uso incluyen enviar datos a Kafka desde cualquier aplicación frontend desarrollada en cualquier lenguaje, ingresar datos en un marco de trabajo de procesamiento en tiempo real que aún no soporta Kafka, y crear scripts para realizar acciones administrativas.

## Instalación

Puede descargar versiones ya compiladas del Kafka REST Proxy como parte de la [Confluent Platform](https://www.confluent.io/product/confluent-platform/).

Puede consultar nuestras completas [instrucciones de instalación](http://docs.confluent.io/current/installation.html#installation) y toda la [documentación](http://docs.confluent.io/current/kafka-rest/docs/).


Para instalar desde el código fuente, siga las instrucciones de la sección de Desarrollo a continuación.

## Despliegue

Kafka REST Proxy incluye un servidor Jetty integrado y puede ser desplegado una vez configurado para conectarse a un clúster Kafka existente.

Al ejecutar ``mvn clean package`` se ejecutan los 3 objetivos de compilación.
- El objetivo ``development`` compila todas las dependencias necesarias en una subcarpeta ``kafka-rest/target`` sin empaquetarlas en un formato distribuible. A continuación, se pueden utilizar los scripts de envoltura ``bin/kafka-rest-start`` y ``bin/kafka-rest-stop`` para iniciar y detener el servicio.
- El objetivo ``package`` está diseñado para ser utilizado en entornos con dependencias compartidas y omite algunas dependencias que se supone deben proporcionarse desde el exterior. Compila las demás dependencias tanto en una subcarpeta ``kafka-rest/target`` como en archivos archivados distribuibles. Luego, se pueden utilizar los scripts de envoltura ``bin/kafka-rest-start`` y ``bin/kafka-rest-stop`` para iniciar y detener el servicio.
- El objetivo ``standalone`` empaqueta todas las dependencias necesarias en un JAR distribuible que se puede ejecutar de forma estándar (``java -jar $base-dir/kafka-rest/target/kafka-rest-X.Y.Z-standalone.jar``).

## Inicio rápido (API v3)

Lo que sigue parte del supuesto de que ya cuenta con Kafka y una instancia del REST Proxy en ejecución con la configuración predeterminada, además de que ya se hayan creado algunos temas.

La API v3 es la versión más reciente de la API. El ID del clúster es un parámetro de ruta que permite que el REST Proxy funcione con múltiples clústeres de Kafka. Las respuestas de la API suelen contener enlaces a recursos relacionados, como la lista de particiones de un tema. El tipo de contenido siempre es `application/json`.

### Obtener la información del clúster local
```bash
$ curl http://localhost:8082/v3/clusters

Respuesta:
  {"kind":"KafkaClusterList",
   "metadata":{"self":"http://localhost:8082/v3/clusters","next":null},
   "data":[
    {"kind":"KafkaCluster",
     "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q",
     "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q"},
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "controller":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/brokers/0"},
     "acls":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/acls"},
     "brokers":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/brokers"},
     "broker_configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/broker-configs"},
     "consumer_groups":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/consumer-groups"},
     "topics":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics"},
     "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/-/partitions/-/reassignment"}
    }
   ]
  }
```

El ID del clúster en la salida es `xFhUvurESIeeCI87SXWR-Q`.

### Obtener una lista de temas
```bash
$ curl http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

Respuesta:
  {"kind":"KafkaTopicList",
   "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics","next":null},
   "data":[
    {"kind":"KafkaTopic",
     "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest",
     "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q/topic=jsontest"},
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "is_internal":false,
     "replication_factor":1,
     "partitions_count":1,
     "partitions":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions"},
     "configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/configs"},
     "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions/-/reassignment"}
    }
   ]
  }
```

### Crear un tema
```bash
$ curl -X POST -H "Content-Type:application/json" -d '{"topic_name":"jsontest"}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

Respuesta:
  {"kind":"KafkaTopic",
   "metadata":{"self":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest",
   "resource_name":"crn:///kafka=xFhUvurESIeeCI87SXWR-Q/topic=jsontest"},
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "is_internal":false,
   "replication_factor":1,
   "partitions_count":1,
   "partitions":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions"},
   "configs":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/configs"},
   "partition_reassignments":{"related":"http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/partitions/-/reassignment"}
  }
```

### Producir registros con datos JSON
```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"JSON","data":{"name":"testUser"}}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":0,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":19}
  }
```

En la respuesta, el código de error 200 es un código de estado HTTP (OK) que indica que la operación tuvo éxito. Dado que se puede utilizar esta API para enviar múltiples registros a un tema como parte de la misma solicitud, cada registro generado cuenta con su propio código de error. Para enviar varios registros, basta con concatenarlos de la siguiente manera:

```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"JSON","data":"ONE"}} {"value":{"type":"JSON","data":"TWO"}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":1,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":5}
  }
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":2,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"JSON","size":5}
  }
```

### Generar registros con datos de tipo cadena
```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"value":{"type":"STRING","data":"REST"}}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records

Response:
  {"error_code":200,
   "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
   "topic_name":"jsontest",
   "partition_id":0,
   "offset":2,
   "timestamp":"2023-03-09T14:07:23.592Z",
   "value":{"type":"STRING","size":4}
  }
```

Los datos se tratan como una cadena en codificación UTF-8 y siguen las reglas de JSON para escapar caracteres especiales.

### Generar registros por lotes

Como alternativa al modo de transmisión en tiempo real, puede generar varios registros de forma por lotes. Aunque esto no se considera transmisión en tiempo real, resulta más sencillo de utilizar con bibliotecas HTTP que esperan un comportamiento de solicitud-respuesta directo.

Cada entrada del lote cuenta con un identificador único (una cadena de hasta 80 caracteres) que se puede utilizar para relacionar las respuestas. Los identificadores de las entradas dentro de un lote deben ser exclusivos.

```bash
$ curl -X POST -H "Content-Type: application/json" \
       -d '{"entries":[{"id":"first","value":{"type":"JSON","data":"ONE"}}, {"id":"second","value":{"type":"JSON","data":"TWO"}}]}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics/jsontest/records:batch

Response:
  {"successes":[
    {"id":"first",
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "partition_id":0,
     "offset":3,
     "timestamp":"2023-03-09T14:07:23.592Z",
     "value":{"type":"JSON","size":5}
    },
    {"id":"second",
     "cluster_id":"xFhUvurESIeeCI87SXWR-Q",
     "topic_name":"jsontest",
     "partition_id":0,
     "offset":4,
     "timestamp":"2023-03-09T14:07:23.592Z",
     "value":{"type":"JSON","size":5}
    }
   ],
   "failures":[]
  }
```

Los éxitos y los fracasos se devuelven en la respuesta en arrays separados de esta manera:

```json
{
  "successes": [
    {
      "id": "1",
      "cluster_id": "xFhUvurESIeeCI87SXWR-Q",
      "topic_name": "jsontest",
      "partition_id": 0,
      "offset": 5,
      "timestamp": "2023-03-09T14:07:23.592Z",
      "value": {
        "type": "STRING",
        "size": 7
      }
    }
  ],
  "failures": [
    {
      "id": "2",
      "error_code": 400,
      "message": "Solicitud inválida: el dato \"Message$\" no es una cadena base64 válida."
    }
  ]
}
```

## Inicio rápido (API v2)
La anterior API v2 es un poco más concisa.

### Obtener una lista de temas
```bash
$ curl http://localhost:8082/topics
  
Respuesta:
  ["__consumer_offsets","jsontest"]
```

### Obtener información sobre un tema
```bash
$ curl http://localhost:8082/topics/jsontest

Respuesta:
  {"name":"jsontest",
   "configs":{},
   "partitions":[
    {"partition":0,
     "leader":0,
     "replicas":[
      {"broker":0,
       "leader":true,
       "in_sync":true
      }
     ]
    }
   ]
  }
```

### Producir registros con datos JSON
```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" \
       -d '{"records":[{"value":{"name": "testUser"}}]}' \
       http://localhost:8082/topics/jsontest

Response:
  {"offsets":[
    {"partition":0,
     "offset":0,
     "error_code":null,
     "error":null
    }
   ],
   "key_schema_id":null,
   "value_schema_id":null
  }
```

### Consumir datos JSON
Primero, cree un consumidor para datos JSON que comience desde el principio del tema. El grupo de consumidores se llama `my_json_consumer` y la instancia es `my_consumer_instance`.

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
       -d '{"name": "my_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
       http://localhost:8082/consumers/my_json_consumer

Response:
  {"instance_id":"my_consumer_instance",
   "base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance"
  }
```

Suscriba al consumidor a un tema.

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
       -d '{"topics":["jsontest"]}' \
      http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/subscription

Response:
  # No hay contenido en la respuesta
```

Luego, consuma algunos datos de un tema utilizando la URL base que se muestra en la primera respuesta.

```bash
$ curl -X GET -H "Accept: application/vnd.kafka.json.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/records

Response:
  [
   {"key":null,
    "value":{"name":"testUser"},
    "partition":0,
    "offset":0,
    "topic":"jsontest"
   }
  ]
```

Finalmente, cierre el consumidor con una solicitud DELETE para que abandone el grupo y libere sus recursos.  
```bash    
$ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance

Response:
  # No content in response
```

## Desarrollo

Para compilar una versión de desarrollo, es posible que necesite las versiones de desarrollo de [common](https://github.com/confluentinc/common), [rest-utils](https://github.com/confluentinc/rest-utils) y [schema-registry](https://github.com/confluentinc/schema-registry). Una vez instaladas estas dependencias, podrá compilar el Kafka REST Proxy con Maven. Todas las fases estándar del ciclo de vida funcionan normalmente.

Puede evitar compilar versiones de desarrollo de las dependencias al trabajar con la etiqueta de versión más reciente (o anterior), o con la rama `<release>-post`, las cuales harán referencia a dependencias ya compiladas y disponibles en el [repositorio público](http://packages.confluent.io/maven/). Por ejemplo, la rama `7.3.0-post` puede utilizarse como base para los parches de esta versión.

## Contribuir

- Código fuente: https://github.com/confluentinc/kafka-rest
- Seguimiento de problemas: https://github.com/confluentinc/kafka-rest/issues

## Licencia

Este proyecto está licenciado bajo la [Confluent Community License](LICENSE).
