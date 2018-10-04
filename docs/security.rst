.. _kafkarest_security:

|crest| Security
================

|crest| supports security features, including:

* :ref:`SSL <kafka-rest-https-config>` for securing communication between REST clients and the |crest| (HTTPS)
* :ref:`SSL encryption <encryption-ssl-rest-proxy>` between the |crest| and a secure Kafka cluster
* :ref:`SSL authentication<authentication-ssl-rest-proxy>` between the |crest| and a secure Kafka Cluster
* :ref:`SASL authentication<kafka_sasl_auth>` between the |crest| and a secure Kafka Cluster

For more configuration details, check the :ref:`configuration options<kafkarest_config>`.

.. tip: For an example that includes the full |crest| security configuration see the :ref:`Confluent Platform demo <cp-demo>`.

By default, all the requests to the broker use the same Kerberos Principal or the SSL certificate
to communicate with the broker when the ``client.security.protocol`` is configured to be either
of SSL, SASL_PLAIN, or SASL_SSL. With this behavior, it's not possible to set fine-grained ACL's for
individual topics. This behavior can be modified by using the Confluent Security Plugins. Refer to
:ref:`|crest| Security Plugins<confluentsecurityplugins_kafaka_rest_security_plugin>` for more
details.
