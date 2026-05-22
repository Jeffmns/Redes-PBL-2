# 🚁 Sistema de Coordenação de Drones Marítimos - Estreito de Ormuz

Este repositório contém a infraestrutura distribuída para coordenação de drones autônomos de resgate e monitoramento marítimo. Desenvolvido para a disciplina TEC502: MI - Concorrência e Conectividade.

## Arquitetura da Solução

O sistema foi desenhado para não possuir **nenhum ponto único de falha (SPOF)** e é composto por 4 grandes blocos:

1. **Sensores (Publishers):** Simulam boias e radares no oceano. Enviam alertas de emergência (JSON) com coordenadas, setor afetado e nível de gravidade.
2. **Drones (Subscribers/Publishers):** Aguardam ordens de despacho tático e reportam constantemente seu status operacional (`LIVRE` ou `OCUPADO`).
3. **Controladores (Cérebro Distribuído):** Componentes P2P responsáveis por manter a fila de prioridades e despachar os drones. Eles se comunicam internamente para garantir o gerenciamento da missão e o estado global da frota.
4. **Cluster MQTT (EMQX):** Três brokers em rede encarregados de rotear as mensagens assíncronas entre os Sensores, Drones e Controladores de forma altamente disponível.

## Concorrência e Consenso Distribuído

Para evitar que dois drones sejam enviados para a mesma ocorrência (garantia de exclusão mútua), os **Controladores** implementam um algoritmo de consenso inspirado no **Raft**, utilizando chamadas **RPC (Remote Procedure Call)** sobre protocolo TCP puro.

* **Eleição de Líder:** Os nós do cluster votam entre si. Apenas o Controlador em estado de **Líder** processa os eventos do MQTT, interage com a fila de alertas e despacha ordens aos drones.
* **Fila de Prioridade:** Os eventos que excedem a capacidade de atendimento imediato são ordenados em memória pela `Gravidade` da emergência e desempatados pelo tempo de chegada (`Timestamp` ISO 8601).
* **Self-Healing e Resiliência:** Se o Líder falhar, a comunicação RPC utiliza *Timeouts* de 500ms que evitam travamentos em cascata. Os seguidores detectam o silêncio e iniciam uma nova eleição quase instantaneamente.

## Como Executar Localmente (Modo Desenvolvimento)

Se você não tiver um arquivo `.env` configurado, o sistema assume valores padrões (default fallbacks) para rodar inteiramente de forma local na rede isolada do Docker.

```bash
docker compose up --
```

O sistema realiza todas as ações automaticamente, não possuindo um terminal interativo, pois isso simularia melhor as solicitações dos setores acontecendo de forma autônoma de acordo aos sensores.