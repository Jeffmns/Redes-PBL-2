package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Estrutura do drone
type Drone struct {
	ID     string
	Status string // "LIVRE", "OCUPADO"
	Setor  string
}

// Estruturas usadas para mandar mensagens via RPC (Rede Interna)
type RequisicaoLog struct {
	DroneID string
	Status  string
	Setor   string
}
type RespostaLog struct {
	Sucesso bool
}

// ServicoRaft é o que vamos expor na rede para os outros PCs chamarem
type ServicoRaft struct {
	Controlador *Controlador
}

// SincronizarLog é chamado via rede pelo Líder nos Seguidores
func (s *ServicoRaft) SincronizarLog(req *RequisicaoLog, res *RespostaLog) error {
	s.Controlador.Mutex.Lock()
	defer s.Controlador.Mutex.Unlock()

	// O seguidor anota no seu próprio caderninho
	drone := s.Controlador.Frota[req.DroneID]
	drone.Status = req.Status
	drone.Setor = req.Setor

	fmt.Printf("[RPC - Seguidor] Registro atualizado! %s agora está %s no %s\n", req.DroneID, req.Status, req.Setor)
	res.Sucesso = true
	return nil
}

// O CONTROLADOR E O MQTT
type Controlador struct {
	ID         string
	EstadoRaft string
	Frota      map[string]*Drone
	Mutex      sync.Mutex
	MqttClient mqtt.Client
	PeersRPC   []string // Lista de IPs/Portas dos outros controladores
}

// Inicia o servidor RPC para ficar escutando os outros controladores
func (c *Controlador) IniciarServidorRPC(porta string) {
	servico := &ServicoRaft{Controlador: c}
	rpc.Register(servico)

	listener, err := net.Listen("tcp", ":"+porta)
	if err != nil {
		log.Fatalf("Erro ao iniciar RPC na porta %s: %v", porta, err)
	}

	fmt.Println("[Rede Interna] Servidor RPC Raft rodando na porta", porta)
	go rpc.Accept(listener)
}

// Tratativa quando o MQTT recebe uma mensagem de alerta
func (c *Controlador) AoReceberAlertaMQTT(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("\n🚨 [MQTT] Alerta recebido no tópico %s: %s\n", msg.Topic(), msg.Payload())

	if c.EstadoRaft != "LIDER" {
		fmt.Println(" Mensagem recebida no seguidor, não há ação direta.")
		return
	}

	c.Mutex.Lock()
	// Acha drone livre
	var droneEscolhido *Drone
	for _, d := range c.Frota {
		if d.Status == "LIVRE" {
			droneEscolhido = d
			break
		}
	}
	c.Mutex.Unlock() // Libera rápido para não travar tudo

	if droneEscolhido != nil {
		// Prepara o log para mandar aos seguidores através do RPC
		req := RequisicaoLog{DroneID: droneEscolhido.ID, Status: "OCUPADO", Setor: string(msg.Payload())}

		fmt.Println(" -> [Raft] Solicitação para que os seguidores registrem o log...")
		votosPositivos := 1 // O Líder já vota sim automaticamente

		// Dispara requisições RPC para os outros nós
		for _, peerAddress := range c.PeersRPC {
			clienteRPC, err := rpc.Dial("tcp", peerAddress)
			if err != nil {
				fmt.Printf("    - Falha ao contatar nó %s\n", peerAddress)
				continue
			}

			var resposta RespostaLog
			// Chama a função 'SincronizarLog' remotamente no outro computador!
			err = clienteRPC.Call("ServicoRaft.SincronizarLog", &req, &resposta)
			if err == nil && resposta.Sucesso {
				votosPositivos++
			}
			clienteRPC.Close()
		}

		// Verifica o quórum (maioria dos votos) para decidir se confirma ou não a ação
		maioria := (len(c.PeersRPC)+1)/2 + 1
		if votosPositivos >= maioria {
			// Atualiza o próprio log do Líder
			c.Mutex.Lock()
			droneEscolhido.Status = "OCUPADO"
			droneEscolhido.Setor = string(msg.Payload())
			c.Mutex.Unlock()

			// PUBLICA NO MQTT A ORDEM FINAL
			topicoCmd := fmt.Sprintf("drones/cmd/%s", droneEscolhido.ID)
			fmt.Printf("Maioria dos seguidores confirmou! Despachando %s via MQTT!\n", droneEscolhido.ID)
			c.MqttClient.Publish(topicoCmd, 1, false, string(msg.Payload()))
		} else {
			fmt.Println("Falha no consenso. Ação abortada.")
		}
	}
}

func main() {
	// Lê as variáveis de ambiente passadas pelo Docker
	meuID := os.Getenv("CONTROLLER_ID")
	minhaPorta := os.Getenv("CONTROLLER_PORT")
	estadoInicial := os.Getenv("ESTADO_RAFT") // Obtém o que cada controlador é (Líder ou Seguidor)
	peersRaw := os.Getenv("PEERS_RPC")

	// Separa a lista de amigos (peers) por vírgula
	listaPeers := []string{}
	if peersRaw != "" {
		listaPeers = strings.Split(peersRaw, ",")
	}

	fmt.Printf("Iniciando %s na porta %s (Estado: %s). Peers: %v\n", meuID, minhaPorta, estadoInicial, listaPeers)

	// Cria o controlador dinamicamente
	meuControlador := &Controlador{
		ID:         meuID,
		EstadoRaft: estadoInicial,
		Frota: map[string]*Drone{
			"drone_01": {ID: "drone_01", Status: "LIVRE", Setor: "base"},
		},
		PeersRPC: listaPeers,
	}

	// Inicia o servidor RPC na porta especificada
	meuControlador.IniciarServidorRPC(minhaPorta)

	// Configura o MQTT (Rede de Sensores/Drones)
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://broker-mqtt:1883") // Endereço do broker local
	opts.SetClientID("Controlador_" + meuControlador.ID)

	meuControlador.MqttClient = mqtt.NewClient(opts)

	fmt.Println("[MQTT] Tentando conectar ao Broker...")
	for {
		token := meuControlador.MqttClient.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		fmt.Printf("[MQTT] Broker ainda não está pronto. Tentando de novo em 2s... (Erro: %v)\n", token.Error())
		time.Sleep(2 * time.Second)
	}
	fmt.Println("[MQTT] Conectado ao Broker com sucesso!")

	// Assina o tópico de emergências apontando para nossa função callback
	meuControlador.MqttClient.Subscribe("setor/+/emergencia", 1, meuControlador.AoReceberAlertaMQTT)

	// Mantém rodando
	select {}
}
