# Kafka REST Proxy

<!-- hy-mt2-i18n:start -->
[English](./README.md) | [中文](./README_zh-CN.md) | **日本語** | [Español](./README_es.md)
<!-- hy-mt2-i18n:end -->


Kafka REST Proxyは、KafkaクラスターへのRESTfulなインターフェースを提供します。これにより、ネイティブなKafkaプロトコルやクライアントを使用することなく、データの生成や消費、クラスターの状態の確認、および管理操作を簡単に行うことができます。利用事例としては、あらゆる言語で構築されたフロントエンドアプリからKafkaにデータを報告したり、まだKafkaをサポートしていないストリーム処理フレームワークにデータを取り込んだり、管理操作をスクリプト化したりすることが挙げられます。

## インストール

[Confluent Platform](https://www.confluent.io/product/confluent-platform/) の一部として、Kafka REST Proxy の事前構築済みバージョンをダウンロードできます。

完全な[インストール手順](http://docs.confluent.io/current/installation.html#installation)や包括的な[ドキュメント](http://docs.confluent.io/current/kafka-rest/docs/)は、こちらでご覧いただけます。


ソースからインストールするには、以下の「開発」セクションに記載されている手順に従ってください。

## 配置展開

Kafka REST Proxyには組み込みのJettyサーバーが含まれており、既存のKafkaクラスターに接続するよう設定した後でデプロイすることができます。

``mvn clean package`` を実行すると、3つのアセンブリターゲットがすべて実行されます。
- ``development`` ターゲットでは、必要なすべての依存関係が ``kafka-rest/target`` というサブフォルダ内にまとめられますが、配布可能な形式でパッケージ化されることはありません。その後、ラッパースクリプトである ``bin/kafka-rest-start`` および ``bin/kafka-rest-stop`` を使用してサービスの起動・停止を行えます。
- ``package`` ターゲットは共有依存関係環境で使用されることを想定しており、外部から提供されると想定される一部の依存関係は除外されます。他の依存関係も ``kafka-rest/target`` というサブフォルダ内および配布可能なアーカイブ内にまとめられます。その後、ラッパースクリプトである ``bin/kafka-rest-start`` および ``bin/kafka-rest-stop`` を使用してサービスの起動・停止を行えます。
- ``standalone`` ターゲットでは、必要なすべての依存関係が標準的に実行可能な配布可能な JAR 形式（``java -jar $base-dir/kafka-rest/target/kafka-rest-X.Y.Z-standalone.jar``）でパッケージ化されます。

## クイックスタート (v3 API)

以下の内容は、Kafkaおよびデフォルト設定で動作しているREST Proxyのインスタンスが既に存在し、いくつかのトピックも作成済みであることを前提としています。

v3 APIはこのAPIの最新バージョンです。クラスターIDは、REST Proxyが複数のKafkaクラスターを扱えるようにするためのパスパラメーターです。APIの応答には、トピックのパーティション一覧など、関連するリソースへのリンクが含まれることが多いです。コンテンツタイプは常に`application/json`です。

### ローカルクラスター情報の取得
```bash
$ curl http://localhost:8082/v3/clusters

Response:
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

出力されるクラスターIDは`xFhUvurESIeeCI87SXWR-Q`です。

### トピックの一覧を取得する
```bash
$ curl http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

Response:
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

### トピックの作成
```bash
$ curl -X POST -H "Content-Type:application/json" -d '{"topic_name":"jsontest"}' \
       http://localhost:8082/v3/clusters/xFhUvurESIeeCI87SXWR-Q/topics

応答:
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

### JSONデータを使用してレコードをプロデュースする
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

応答内の`error_code`が200である場合、これは操作が成功したことを示すHTTPステータスコード（OK）です。このAPIを使用して同じリクエスト内で複数のレコードをトピックにストリーミングできるため、生成される各レコードにはそれぞれ独自のエラーコードが設定されます。複数のレコードを送信するには、このようにレコードを連結するだけです。

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

### 文字列データを持つレコードの生成
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

データはUTF-8エンコーディングされた文字列として扱われ、特殊文字のエスケープにはJSONの規則が適用されます。

### バッチでレコードを生成する

ストリーミングモードの代替として、複数のレコードをバッチで生成することもできます。これはストリーミングではありませんが、シンプルなリクエスト・レスポンス動作を期待するHTTPライブラリを使用する際により使いやすくなります。

バッチ内の各エントリーには一意な識別子（80文字までの文字列）が付けられており、これを使ってレスポンスを照合することができます。バッチ内のすべてのエントリーの識別子は一意でなければなりません。

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

応答において、成功例と失敗例はこのように別々の配列として返されます：

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
      "message": "Bad Request: data=\"Message$\" は有効な base64 文字列ではありません。"
    }
  ]
}
```

## クイックスタート (v2 API)
以前のv2 APIの方がより簡潔です。

### トピックの一覧を取得する
```bash
$ curl http://localhost:8082/topics

応答:
  ["__consumer_offsets","jsontest"]
```

### 1つのトピックに関する情報を取得する
```bash
$ curl http://localhost:8082/topics/jsontest

Response:
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

### JSONデータを使ってレコードをプロデュースする
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

### JSONデータの消費
まず、トピックの先頭からJSONデータを消費するためのコンシューマーを作成します。このコンシューマーグループの名前は`my_json_consumer`で、インスタンス名は`my_consumer_instance`です。

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
       -d '{"name": "my_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
       http://localhost:8082/consumers/my_json_consumer

Response:
  {"instance_id":"my_consumer_instance",
   "base_uri":"http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance"
  }
```

コンシューマをトピックにサブスクライブします。

```bash
$ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
       -d '{"topics":["jsontest"]}' \
      http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance/subscription

Response:
  # 応答に内容はありません
```

次に、最初の応答に含まれるベースURLを使用して、トピックからデータを取得します。

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

最後に、DELETEリクエストを送信してコンシューマーを終了させ、グループから外れてリソースを解放します。  
```bash    
$ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
       http://localhost:8082/consumers/my_json_consumer/instances/my_consumer_instance

Response:
  # No content in response
```

## 開発版

開発版をビルドするには、[common](https://github.com/confluentinc/common)、[rest-utils](https://github.com/confluentinc/rest-utils)、[schema-registry](https://github.com/confluentinc/schema-registry) の開発版が必要になる場合があります。これらをインストールした後、Maven を使って Kafka REST Proxy をビルドできます。すべての標準的なライフサイクルフェーズが正常に動作します。

[public repository](http://packages.confluent.io/maven/) から事前にビルド済みの依存関係を参照する、最新（またはそれ以前の）リリースタグや `<release>-post` ブランチを基にビルドすることで、依存関係の開発版を自動的にビルドする必要を避けることができます。例えば、ブランチ `7.3.0-post` をこのバージョン向けパッチのベースとして使用できます。

## 貢献するには

- ソースコード: https://github.com/confluentinc/kafka-rest
- 問題追跡ツール: https://github.com/confluentinc/kafka-rest/issues

## ライセンス

このプロジェクトは、[Confluent Community License](LICENSE) のもとでライセンスされています。
