## References
- https://developer.confluent.io/courses/kafka-streams/hands-on-processor-api/
- https://kafka.apache.org/21/documentation/streams/developer-guide/testing.html
- https://developer.confluent.io/courses/kafka-streams/hands-on-testing/



## Todo
1. Colocar um array de fraudes para cada tx
2. No final da stream gravar em stream compactada o estado da das transações e das que participaram da mesma janela de análse, de modo a atualizar a stream compactada se a transação é fraude ou nao baseado no comportamento da janeal