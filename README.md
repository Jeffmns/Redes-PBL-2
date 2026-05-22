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

## Como executar localmente

Se você não tiver um arquivo `.env` configurado, o sistema assume valores padrões (default fallbacks) para rodar inteiramente de forma local na rede isolada do Docker.

```bash
docker-compose up --
```

O sistema realiza todas as ações automaticamente, não possuindo um terminal interativo, pois isso simularia melhor as solicitações dos setores acontecendo de forma autônoma de acordo aos sensores.

## Como executar em múltiplas máquinas

Para distribuir o sistema entre os computadores, utilizamos as variáveis de ambiente.

1. Na raiz do projeto de **cada computador**, crie um arquivo `.env` mapeando os IPs físicos correspondentes. Exemplo:

    # IPs do MQTT
    IP_EMQX_1=172.16.103.4
    PORT_EMQX_1=1883
    IP_EMQX_2=172.16.103.4
    PORT_EMQX_2=1884
    IP_EMQX_3=172.16.103.4
    PORT_EMQX_3=1885
    
    # IPs dos Controladores na rede física
    IP_CTRL_1=172.16.103.3
    PORT_CTRL_1=9001
    IP_CTRL_2=172.16.103.2
    PORT_CTRL_2=9002
    IP_CTRL_3=172.16.103.2
    PORT_CTRL_3=9003

2. Suba os serviços em cada máquina individualmente utilizando a flag `--no-deps`. Isso força o Docker a iniciar apenas o que você pedir, ignorando o restante da topologia que está fisicamente em outro PC.

**Na Máquina Central do Cluster MQTT:**

    docker-compose up emqx1 emqx2 emqx3

**Nas Máquinas dos Nós (Controladores, Sensores e Drones):**

    docker-compose up --no-deps controlador-1 sensor-norte drone-01
    docker-compose up --no-deps controlador-2 controlador-3 sensor-sul drone-02

## Teste de Confiabilidade

Para provar a robustez arquitetural e a tolerância a falhas na prática, você pode derrubar propositalmente o principal da operação executando:

    docker-compose stop controlador-1

Você poderá observar nos logs em tempo real que os Controladores Seguidores iniciarão uma nova eleição, um novo Líder vai assumir o controle do processamento, e o sistema vai continuar operando sem perder eventos e sem duplicar o envio de drones.