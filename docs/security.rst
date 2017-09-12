.. _kafkarest_security:

Security Overview
-----------------

REST Proxy supports SSL for securing communication between REST clients and the REST Proxy (HTTPS), and both SSL and SASL to secure communication between REST Proxy and Apache Kafka.

For more details, check the :ref:`configuration options<kafkarest_config>`.

By default, all the requests to the broker use the same Kerberos Principal or the SSL certificate
to communicate with the broker when the ``client.security.protocol`` is configured to be either
of SSL, SASL_PLAIN or SASL_SSL. With this behvior, its not possible to set fine-grained ACL's for
individual topics. This behavior can be modified by using the Confluent Security Plugins. Refer to
:ref:`Kafka Rest Security Plugins<confluentsecurityplugins_kafaka_rest_security_plugin>` for more
details.
