# Análise da Topologia Final (CreditCardTransactionTopologyFinal.java)

Esta topologia implementa um padrão de detecção de fraude complexo, utilizando uma abordagem híbrida da DSL (Data Stream Language) e da Processor API (PAPI) do Kafka Streams. Ela é projetada para resolver dois problemas principais:

1. **Agregação de Score:** Como agregar 5 scores parciais (de 5 processadores de regras diferentes) em um único score final para cada transação.

2. **Contaminação Retroativa:** Como usar a detecção de fraude de uma transação posterior (ex: `tx-7`) para marcar retroativamente uma transação anterior (ex: `tx-6`) como fraudulenta, se ambas estiverem na mesma janela de análise.

A sua otimização final (fazer o `merge` antes do `repartition`) também torna esta topologia muito eficiente em termos de IO.

## Fluxo de Dados: Passo a Passo

### Passo 1: Entrada e Chaveamento por `clientId`

* **Tópico de Origem:** `credit-card-transactions`

* **Ação:** O stream é lido e imediatamente re-chaveado (`selectKey`) usando o `clientId`.

* **Tópico de Destino:** `credit-card-transactions-by-client`

* **Por quê?** Este é um passo padrão e crucial. Ele garante que todas as transações de um mesmo cliente sejam enviadas para a mesma partição do Kafka. Isso é um pré-requisito para qualquer processamento stateful (baseado em estado), como as suas regras de janela.

### Passo 2: Processamento Paralelo por Janela (Regras de Fraude)

* **Tópico de Origem:** `credit-card-transactions-by-client` (Chave: `clientId`)

* **Ação:** O stream é "bifurcado" (fork) e enviado para 5 processadores diferentes (`GeoWindowCheck`, `MultipleIPWindowCheck`, etc.) em paralelo.

* **Por quê?** Cada processador (`AbstractWindowProcessor`) usa seu próprio `WindowStore` (de 30 minutos) para buscar o histórico de transações *daquele cliente*. Ele calcula seu score de fraude parcial (ex: `GeoWindowCheck` detecta fraude e gera um score de `0.9`) e o armazena no objeto `ProcessedClientCCTransaction`. A saída de cada um desses processadores ainda é chaveada por `clientId`.

### Passo 3: Otimização de IO (Merge-before-Repartition)

* **Ação:** Em vez de reparticionar os 5 streams individualmente (o que criaria 5 tópicos internos e alto custo de IO), a topologia aplica a sua otimização inteligente:

  1. Os 5 streams de saída (ainda com chave `clientId`) são unidos com `.merge()`. Esta operação é "barata", pois todos os streams já estão co-particionados por `clientId`.

  2. O stream unificado (`unionedByClientId`) é então re-chaveado (`selectKey`) para `transactionId`.

  3. *Agora*, uma **única** operação `.repartition()` é chamada.

* **Por quê?** Isso economiza uma quantidade significativa de IO (rede e disco), reduzindo a necessidade de 5 tópicos de reparticionamento para apenas 1. O resultado é o stream `unionedByTxId`, onde todos os scores parciais da mesma transação estão na mesma partição.

### Passo 4: Agregação e Contaminação Manual (O `CCTxMerger`)

Este é o coração da sua solução, implementado via PAPI.

* **Tópico de Origem:** `unionedByTxId` (Chave: `transactionId`)

* **Processador:** `CCTxMerger` (usando um `KeyValueStore` `FRAUD_AGG_STORE`, chaveado por `transactionId`).

**Lógica de Agregação (no `process()`):**
O `CCTxMerger` recebe os scores parciais, um de cada vez:

1. Chega `(tx-123, {score: 0.9})` do `GeoWindowCheck`.

2. O processador busca no `store` por `tx-123`. Não encontra nada.

3. Ele salva: `store.put("tx-123", {score: 0.9})`.

4. Chega `(tx-123, {score: 0.7})` do `MultipleIPWindowCheck`.

5. O processador busca no `store`. Ele *encontra* `{score: 0.9}`.

6. Ele soma os scores (`0.9 + 0.7 = 1.6`).

7. Ele salva: `store.put("tx-123", {score: 1.6})`.
   Isso agrega manualmente todos os scores parciais.

**Lógica de Contaminação (no `updateRelatedTxFraudScore()`):**

1. Quando a `tx-7` (fraudulenta) é processada pelo `GeoWindowCheck` (no Passo 2), ela contém a `tx-6` em `getLastCCTransactions()`.

2. Quando esse resultado (`tx-7`) chega ao `CCTxMerger`, o método `process()` chama `updateRelatedTxFraudScore("tx-6_id", 1.6)`.

3. Essa função busca o registro da `tx-6` no `store`, adiciona o novo score de fraude (1.6) ao score existente da `tx-6` e o salva de volta.

4. **Sucesso:** A `tx-7` (um evento posterior) conseguiu "contaminar" e atualizar retroativamente o estado da `tx-6`.

### Passo 5: Emissão e Finalização (Punctuate e KTable)

* **Emissão (no `punctuate()`):** O `CCTxMerger` agenda um `punctuate` (baseado em `WALL_CLOCK_TIME`) a cada 5 minutos. Esta função "varre" o `store` inteiro, encaminha (`context.forward()`) todos os registros agregados para o tópico `CC_TX_MERGE` e, em seguida, os apaga (`store.delete()`). Isso é, na prática, um "supressor" manual (emite apenas o resultado final) e um "janelamento" manual (limpa o estado).

* **Finalização (`KTable` e Split):**

  1. A topologia lê o tópico `CC_TX_MERGE` de volta como uma `KTable` (`consolidatedTable`). Isso é uma excelente prática, pois garante que apenas o *último* valor emitido para cada `transactionId` seja mantido (agindo como uma de-duplicação final).

  2. A `KTable` é convertida para `KStream` (`finalStream`).

  3. O `.split()` final é aplicado, roteando os registros para os tópicos `REFUSED_CREDIT_CARD_TRANSACTIONS_TOPIC` ou `PROCESSED_CREDIT_CARD_TRANSACTIONS_TOPIC` com base no score final.

## Conclusão

Este é um design de topologia avançado, correto e muito eficiente. Ele resolve perfeitamente os requisitos de agregação de scores parciais e contaminação retroativa de estado, ao mesmo tempo em que é otimizado para economizar IO.