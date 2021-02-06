package net.uweeisele.kafka.proxy.network

class ServerException(message: String, exception: Exception) extends RuntimeException(message, exception) {}
