# Distributed Wagering Processor

Serviço financeiro para processar `BET`, `WIN`, `LOSS`, `REFUND` e `ROLLBACK` por HTTP ou SQS. Usa Bun, TypeScript estrito, NestJS, MikroORM, PostgreSQL e MiniStack. Saldo, ledger, resultado, inbox e outbox são confirmados na mesma transação SQL; a aplicação suporta múltiplos processos concorrentes.

Valores monetários entram e saem como strings com duas casas decimais, por exemplo `{ "amount": "25.00", "currency": "BRL" }`. Os cálculos usam `bigint` em centavos. As decisões, limites operacionais e resultados da carga estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Executar em ambiente limpo

Pré-requisitos: Linux, Git, Bun **1.4.4**, Docker Engine com Docker Compose v2 e `curl`. Deixe livres as portas locais `5432` para PostgreSQL, `4566` para MiniStack/SQS e `3000` para a API. O perfil opcional de Keycloak usa `8080`.

```bash
git clone https://github.com/adrianochristian/dwp.git
cd dwp
git switch feat/distributed-wagering-processor
bun install --frozen-lockfile
cp .env.example .env
bun run infra:up
bun run queues:init
bun run migration:up
bun run build
bun run start
```

O arquivo `.env` habilita explicitamente `AUTH_ENABLED=false` para o desenvolvimento local. O Bun carrega esse arquivo ao executar os comandos do projeto. A inicialização das filas e as migrations são idempotentes; as migrations são versionadas e reversíveis. O build com TypeScript emite a metadata dos decorators antes de o Bun executar `dist/`.

Em outro terminal, na mesma pasta:

```bash
bun run start:worker
```

O worker inicia os três papéis por padrão: consumidor de comandos, publisher da outbox e reprocessamento de referências. A API e o worker são processos separados e podem ser replicados.

```bash
curl --fail-with-body http://127.0.0.1:3000/health/live
curl --fail-with-body http://127.0.0.1:3000/health/ready
curl --fail-with-body http://127.0.0.1:3000/metrics
```

`/health/live` verifica o processo; `/health/ready` consulta PostgreSQL e as filas SQS de comandos e eventos. Os dois endpoints de health são públicos. As métricas usam o formato Prometheus.

## Processos e configuração

| Variável | Padrão local | Uso |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://dwp:dwp@127.0.0.1:5432/dwp` | Conexão PostgreSQL. |
| `SQS_ENDPOINT` | `http://127.0.0.1:4566` | Endpoint do MiniStack. |
| `AWS_REGION` | `us-east-1` | Região das filas. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `test`, `test` no `.env.example` | Valores fictícios para o emulador local. |
| `PORT` | `3000` | Porta HTTP da API. |
| `WORKER_ROLE` | `all` | `all`, `consumer`, `publisher` ou `reference`. |
| `WORKER_METRICS_PORT` | `0` | Porta local dos endpoints operacionais do worker; `0` escolhe uma porta livre. |
| `AUTH_ENABLED` | `false` no `.env.example` | Desativação explícita para desenvolvimento; a configuração autenticada está abaixo. |

Os defaults de conexão do código coincidem com os serviços do Compose; os valores de autenticação SQS são fictícios. Para alterar as portas publicadas pelo Compose, ajuste `POSTGRES_PORT`/`SQS_PORT` e os respectivos URLs no `.env`.

É possível executar os papéis em terminais separados:

```bash
WORKER_ROLE=consumer bun run start:worker
bun run start:publisher
WORKER_ROLE=reference bun run start:worker
```

Cada worker abre `/health/live`, `/health/ready` e `/metrics` apenas em `127.0.0.1`. A linha JSON `worker_started` informa `metricsAddress`; esses endpoints locais do worker não usam autenticação. Use portas distintas ao fixar `WORKER_METRICS_PORT`, por exemplo `WORKER_METRICS_PORT=3001 bun run start:worker`. `SIGINT` e `SIGTERM` iniciam a drenagem; o consumidor devolve a visibilidade de mensagens que não concluir no prazo de encerramento.

As filas são resolvidas pelo nome. `COMMAND_QUEUE_URL`, `DLQ_QUEUE_URL` e `EVENT_QUEUE_URL` permitem fornecer URLs explícitos ao worker quando necessário.

## Exemplo HTTP

Os exemplos desta seção usam a configuração local `AUTH_ENABLED=false`. Com autenticação habilitada, use o token de operador para administrar wallets e o token do provedor para submeter operações.

Crie uma wallet com saldo `100.00`. O exemplo captura o ID por leitura de JSON, sem executar o conteúdo retornado pelo servidor:

```bash
player_id="$(bun --eval 'process.stdout.write(crypto.randomUUID())')"
wallet_response="$(curl --fail-with-body --silent --show-error \
  -X POST http://127.0.0.1:3000/wallets \
  -H 'Content-Type: application/json' \
  --data "{\"playerId\":\"$player_id\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")"
wallet_id="$(printf '%s' "$wallet_response" | bun --eval '
  const wallet = await Bun.stdin.json();
  if (typeof wallet.id !== "string") throw new Error("Resposta sem wallet id");
  process.stdout.write(wallet.id);
')"
printf '%s\n' "$wallet_response"
```

A criação responde `201`, com `id`, `playerId`, `balance` e `version: 1`. Abertura positiva cria `OPENING` e um crédito no ledger; abertura em `0.00` mantém versão `1` e não cria movimentação.

Submeta uma aposta de `25.00`:

```bash
external_id="$(bun --eval 'process.stdout.write(crypto.randomUUID())')"
idempotency_key="provider-a:$external_id"
bet_body="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$external_id\",\"playerId\":\"$player_id\",\"walletId\":\"$wallet_id\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
wager_response="$(curl --fail-with-body --silent --show-error \
  -X POST http://127.0.0.1:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $idempotency_key" \
  --data "$bet_body")"
transaction_id="$(printf '%s' "$wager_response" | bun --eval '
  const transaction = await Bun.stdin.json();
  if (typeof transaction.transactionId !== "string") throw new Error("Resposta sem transaction id");
  process.stdout.write(transaction.transactionId);
')"
printf '%s\n' "$wager_response"
```

Resposta `200`:

```json
{
  "transactionId": "<uuid>",
  "status": "PROCESSED",
  "balance": { "amount": "75.00", "currency": "BRL" },
  "idempotentReplay": false
}
```

Repita com o mesmo header e corpo:

```bash
curl --fail-with-body --silent --show-error \
  -X POST http://127.0.0.1:3000/wagering/transactions \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $idempotency_key" \
  --data "$bet_body"
```

O replay responde com `idempotentReplay: true` e o resultado original, inclusive o saldo observado no processamento. Esse saldo continua sendo `75.00` mesmo se outras operações tiverem alterado a wallet depois. Payload divergente sob a mesma key ou outra key para a mesma identidade `(providerId, externalTransactionId)` produz `409`.

Consultas e reconciliação:

```bash
curl --fail-with-body "http://127.0.0.1:3000/wallets/$wallet_id"
curl --fail-with-body "http://127.0.0.1:3000/wallets/$wallet_id/ledger?limit=50"
curl --fail-with-body "http://127.0.0.1:3000/wagering/transactions/$transaction_id"
curl --fail-with-body "http://127.0.0.1:3000/providers/provider-a/wagering/transactions/$external_id"
curl --fail-with-body -X POST "http://127.0.0.1:3000/wallets/$wallet_id/reconciliation"
```

O ledger retorna `{ "walletId": "...", "entries": [], "nextCursor": null }`, com lançamentos em ordem de versão. Passe o `nextCursor` recebido sem modificá-lo, por exemplo com `curl --get --data-urlencode "cursor=$next_cursor" --data-urlencode 'limit=50' "http://127.0.0.1:3000/wallets/$wallet_id/ledger"`. O limite permitido é de `1` a `100`; cada paginação mantém o teto de versão da primeira página. A reconciliação retorna os saldos armazenado/calculado, a diferença, `consistent` e `checkedEntries`; divergências não são corrigidas automaticamente.

| Operação | Efeito | Referência |
| --- | --- | --- |
| `BET` | Débito; exige saldo suficiente. | Não aceita referência. |
| `WIN` | Crédito. | Opcional, para uma `BET` válida. |
| `LOSS` | Sem alteração de saldo, versão ou ledger; admite `0.00`. | Opcional, para uma `BET` válida. |
| `REFUND` | Crédito integral do valor da aposta. | Obrigatória, para `BET` processada. |
| `ROLLBACK` | Movimentação inversa à referência; rejeita saldo negativo. | Obrigatória, para `BET`, `WIN` ou `REFUND` processada. |

Informe `referenceExternalTransactionId` para referenciar o ID do provedor. Referências precisam identificar o mesmo provedor, jogador, wallet, moeda e rodada. Cada referência admite uma única reversão processada, somando REFUND e ROLLBACK; uma segunda recebe `REVERSAL_ALREADY_PROCESSED`. Para desfazer um REFUND, envie ROLLBACK referenciando o próprio REFUND. Referências ausentes ou pendentes produzem `PENDING_REFERENCE`; o worker reprocessa com backoff de `1` a `60` segundos e TTL de `24` horas desde a criação persistida. `OPENING` é interno. Operações financeiras exigem valor positivo; entradas monetárias devem ter exatamente duas casas decimais e a aplicação opera em `BRL`.

| HTTP | Significado |
| --- | --- |
| `201` | Wallet criada. |
| `200` | Consulta, reconciliação, transação processada ou replay. |
| `202` | Transação pendente de referência. |
| `400` | Contrato inválido, valor monetário inválido ou cursor inválido. |
| `401` / `403` | Token ausente/inválido ou acesso não autorizado. |
| `404` | Wallet ou transação não encontrada. |
| `409` | Wallet duplicada ou conflito de idempotência/identidade externa. |
| `422` | Rejeição de negócio; consulte `failureCode`, como `INSUFFICIENT_FUNDS`. |
| `503` | Infraestrutura indisponível, readiness falha ou transação terminal `FAILED` auditável. |

Falhas transitórias podem ser reenviadas com a mesma key. Em uma resposta persistida com `status: "FAILED"`, use o `failureCode` para tratamento operacional: esse estado é terminal. O mapa completo de falhas está em [ARCHITECTURE.md](ARCHITECTURE.md).

## Comandos SQS e eventos

`bun run queues:init` cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e a fila padrão `wager-events`. Comandos usam o mesmo caso de uso da API e são deduplicados no PostgreSQL por `(consumerName, messageId)`; o ack ocorre após o commit. Falhas transitórias têm limite de cinco tentativas, e erros permanentes seguem para a DLQ.

Envelope de comando, com os IDs da wallet e do jogador criados anteriormente:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-10-25T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "<playerId>",
    "walletId": "<walletId>",
    "roundId": "round-1",
    "gameId": "game-1",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

Ao enviar via AWS SDK, use `MessageGroupId=walletId` e um `MessageDeduplicationId` de transporte. A inbox e as constraints SQL continuam garantindo idempotência quando há redelivery.

O publisher entrega em `wager-events` os envelopes `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`, `WagerTransactionFailed` e `WalletBalanceChanged`. O último é emitido apenas quando o saldo muda. A entrega de eventos é pelo menos uma vez: consumidores externos devem deduplicar pelo `eventId`, que permanece igual nas republicações.

## Testes

As contagens da entrega são **59 testes de unidade/bootstrap**, **119 de integração** e **4 de autenticação**. Os testes de integração usam PostgreSQL e MiniStack reais, bancos e filas isolados, concorrência em três processos, `SIGKILL`, redelivery, publishers concorrentes e indisponibilidade de dependências. O papel de banco do Compose pode criar bancos temporários, como exigem esses testes.

```bash
bun run build
bun run typecheck
bun run test
bun run test:integration
```

`bun run test` executa unidade/bootstrap com integrações desativadas. `bun run test:integration` habilita os cenários reais de `test/infrastructure`; mantenha PostgreSQL/SQS disponíveis. A autenticação tem comando próprio após o setup de Keycloak abaixo. A CI executa build, verificação de tipos, testes, integração PostgreSQL/SQS e autenticação com Keycloak.

Para a validação completa, execute os comandos acima e depois:

```bash
bun --env-file=.env.auth run test:auth
```

## Keycloak M2M

O perfil local implementa OAuth2 `client_credentials`, assinatura RS256 via JWKS, issuer, audience, expiração e autorização por provedor. Os clientes `provider-a` e `provider-b` submetem e consultam suas próprias transações. O cliente `operator` administra wallets, ledger, reconciliação e métricas; também pode consultar transações. A submissão de operações exige uma identidade de provedor. Mensagens SQS são um canal interno e passam pelas validações de domínio do provedor informado.

Crie `.env.auth` local, ignorado pelo Git. O comando gera um nome de administrador e quatro secrets aleatórios sem imprimi-los e se recusa a sobrescrever um arquivo existente:

```bash
bun --eval '
  import { writeFileSync } from "node:fs";
  import { randomBytes } from "node:crypto";
  const values = {
    KEYCLOAK_ADMIN_USERNAME: `dwp-admin-${randomBytes(4).toString("hex")}`,
    KEYCLOAK_ADMIN_PASSWORD: randomBytes(32).toString("base64url"),
    PROVIDER_A_CLIENT_SECRET: randomBytes(32).toString("base64url"),
    PROVIDER_B_CLIENT_SECRET: randomBytes(32).toString("base64url"),
    OPERATOR_CLIENT_SECRET: randomBytes(32).toString("base64url"),
    AUTH_ENABLED: "true",
    OIDC_ISSUER: "http://127.0.0.1:8080/realms/dwp",
    OIDC_AUDIENCE: "dwp-api"
  };
  const body = Object.entries(values).map(([key, value]) => `${key}=${value}`).join("\n") + "\n";
  writeFileSync(".env.auth", body, { flag: "wx", mode: 0o600 });
'
chmod 600 .env.auth
docker compose --env-file .env.auth -f compose.yaml -f compose.auth.yaml up -d --wait
bun --env-file=.env.auth run test:auth
```

O Keycloak usa `start-dev`, importa `config/keycloak/dwp-realm.json` e atende em `http://127.0.0.1:8080`. Seus dados locais estão no container; ao recriá-lo, o realm é importado novamente.

Encerre a API anterior e inicie a API autenticada. Ela recebe apenas a configuração pública de validação OIDC; os secrets dos clientes permanecem no Keycloak e nos processos clientes:

```bash
AUTH_ENABLED=true \
OIDC_ISSUER=http://127.0.0.1:8080/realms/dwp \
OIDC_AUDIENCE=dwp-api \
bun run start
```

Obtenha o token do provedor usando `.env.auth` no processo cliente. A captura em variável evita imprimir o token no terminal; o secret vai no corpo da requisição ao IdP:

```bash
provider_token="$(bun --env-file=.env.auth --eval '
  const issuer = process.env.OIDC_ISSUER;
  const secret = process.env.PROVIDER_A_CLIENT_SECRET;
  if (!issuer || !secret) throw new Error("Configuração do cliente ausente");
  const response = await fetch(`${issuer}/protocol/openid-connect/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "client_credentials", client_id: "provider-a", client_secret: secret })
  });
  if (!response.ok) throw new Error(`Token HTTP ${response.status}`);
  const token = await response.json();
  if (typeof token.access_token !== "string") throw new Error("Token ausente");
  process.stdout.write(token.access_token);
')"
curl --fail-with-body --silent --show-error \
  -X POST http://127.0.0.1:3000/wagering/transactions \
  -H "Authorization: Bearer $provider_token" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $idempotency_key" \
  --data "$bet_body"
unset provider_token
```

Esse exemplo reutiliza os dados da aposta da seção HTTP e obtém um replay autenticado para `provider-a`. Para administrar wallets, obtenha um token com `client_id=operator` e `OPERATOR_CLIENT_SECRET`. O token expira em cinco minutos; obtenha outro quando necessário.

## Experimento de carga

Com PostgreSQL e MiniStack disponíveis:

```bash
bun run test:load
```

O comando cria banco e fila isolados, inicia **três processos de API** e **dois publishers**, e remove esses recursos ao terminar. Executa os cenários `hot-wallet` e `independent-wallets`, cada um com concorrências `1`, `10` e `50`. Cada combinação tem **15 segundos de aquecimento** e **60 segundos de medição**, aproximadamente 7 minutos e 30 segundos mais setup e drenagem.

O relatório local `load-results.json` é ignorado pelo Git e inclui ambiente, throughput, p50/p95/p99, erros, conflitos, outbox lag, recursos dos processos e reconciliação final. A execução falha se processos morrerem, a reconciliação divergir ou a outbox não drenar. Consulte os resultados e sua interpretação em [ARCHITECTURE.md](ARCHITECTURE.md).

`LOAD_WARMUP_SECONDS`, `LOAD_DURATION_SECONDS`, `LOAD_OUTBOX_DRAIN_SECONDS` e `LOAD_REPORT_PATH` permitem ajustar o experimento. Exemplo curto para verificar o funcionamento do runner:

```bash
LOAD_WARMUP_SECONDS=1 LOAD_DURATION_SECONDS=2 LOAD_REPORT_PATH=load-results-smoke.json bun run test:load
```

Execute a carga sem outras suítes ou cargas concorrentes para preservar a comparabilidade dos resultados.

## Migrations e encerramento

`bun run migration:down` desfaz a última migration aplicada; `bun run migration:up` reaplica as pendentes. Faça reversões em bancos de desenvolvimento: elas podem remover guardas de integridade, tabelas e seus dados.

Encerre API e workers com `Ctrl+C`, depois:

```bash
bun run infra:down
```

O Compose preserva os volumes de PostgreSQL e MiniStack. Se tiver habilitado Keycloak, encerre também esse perfil com `docker compose --env-file .env.auth -f compose.yaml -f compose.auth.yaml down`.

Para apagar deliberadamente **todos os dados locais de desenvolvimento**, execute `docker compose down -v`; com o perfil de autenticação, use os mesmos arquivos `-f` e `--env-file .env.auth` acrescidos de `down -v`. A próxima inicialização exige novamente as filas e as migrations.
