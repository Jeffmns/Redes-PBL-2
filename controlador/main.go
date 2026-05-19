package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Drone struct {
	ID     string
	Status string
	Setor  string
}

// Estruturas do Log
type RequisicaoLog struct{ DroneID, Status, Setor string }
type RespostaLog struct{ Sucesso bool }

// Estruturas para Eleição e Heartbeat do Raft
type RequisicaoPing struct {
	Termo   int
	LiderID string
}
type RespostaPing struct{ Sucesso bool }

type RequisicaoVoto struct {
	Termo       int
	CandidatoID string
}
type RespostaVoto struct {
	Termo         int
	VotoConcedido bool
}

// ==========================================
// 2. O CONTROLADOR (A Máquina de Estados)
// ==========================================
type Controlador struct {
	ID            string
	EstadoRaft    string // "LIDER", "SEGUIDOR", "CANDIDATO"
	TermoAtual    int    // O "ano" da eleição atual
	VotouEm       string // Em quem eu votei nesta eleição
	UltimoContato time.Time
	Frota         map[string]*Drone
	Mutex         sync.Mutex
	MqttClient    mqtt.Client
	PeersRPC      []string
}

type AlertaSensor struct {
	Setor       string `json:"setor"`
	Coordenadas string `json:"coordenadas"`
	Gravidade   string `json:"gravidade"`
}

type StatusDrone struct {
	DroneID string `json:"drone_id"`
	Status  string `json:"status"`
}

// O SERVIÇO RPC (As funções chamadas via rede)
type ServicoRaft struct{ C *Controlador }

// Sincroniza o Log do Drones (O caderninho)
func (s *ServicoRaft) SincronizarLog(req *RequisicaoLog, res *RespostaLog) error {
	s.C.Mutex.Lock()
	defer s.C.Mutex.Unlock()
	drone := s.C.Frota[req.DroneID]
	drone.Status = req.Status
	drone.Setor = req.Setor
	fmt.Printf("[RPC Log] Atualizado! %s agora está %s no %s\n", req.DroneID, req.Status, req.Setor)
	res.Sucesso = true
	return nil
}

// NOVO: Receber o Ping do Líder (Heartbeat)
func (s *ServicoRaft) Ping(req *RequisicaoPing, res *RespostaPing) error {
	s.C.Mutex.Lock()
	defer s.C.Mutex.Unlock()

	// Se ouviu alguém com um termo (eleição) maior ou igual, reconhece o líder e abaixa a cabeça
	if req.Termo >= s.C.TermoAtual {
		s.C.EstadoRaft = "SEGUIDOR"
		s.C.TermoAtual = req.Termo
		s.C.UltimoContato = time.Now() // Zera o cronômetro da morte!
	}
	res.Sucesso = true
	return nil
}

// NOVO: Receber um pedido de voto de um Candidato
func (s *ServicoRaft) PedirVoto(req *RequisicaoVoto, res *RespostaVoto) error {
	s.C.Mutex.Lock()
	defer s.C.Mutex.Unlock()

	// Se o candidato está numa eleição mais antiga, negamos o voto
	if req.Termo < s.C.TermoAtual {
		res.VotoConcedido = false
		return nil
	}

	// Se a eleição for nova, atualiza o termo e limpa o voto anterior
	if req.Termo > s.C.TermoAtual {
		s.C.TermoAtual = req.Termo
		s.C.EstadoRaft = "SEGUIDOR"
		s.C.VotouEm = ""
	}

	// Se ainda não votei em ninguém neste termo, eu concedo o voto
	if s.C.VotouEm == "" || s.C.VotouEm == req.CandidatoID {
		s.C.VotouEm = req.CandidatoID
		s.C.UltimoContato = time.Now() // Reinicia o cronômetro por segurança
		res.VotoConcedido = true
		fmt.Printf("[Raft] Votei no Candidato %s para o Termo %d!\n", req.CandidatoID, req.Termo)
	} else {
		res.VotoConcedido = false
	}
	res.Termo = s.C.TermoAtual
	return nil
}

// Heartbeats e Eleições
func (c *Controlador) IniciarMotorRaft() {
	for {
		c.Mutex.Lock()
		estadoAtual := c.EstadoRaft
		c.Mutex.Unlock()

		switch estadoAtual {
		case "LIDER":
			// Manda Ping para todo mundo a cada 1 segundo
			req := RequisicaoPing{Termo: c.TermoAtual, LiderID: c.ID}
			for _, peer := range c.PeersRPC {
				go func(p string) {
					clienteRPC, err := rpc.Dial("tcp", p)
					if err == nil {
						clienteRPC.Call("ServicoRaft.Ping", &req, &RespostaPing{})
						clienteRPC.Close()
					}
				}(peer)
			}
			time.Sleep(1 * time.Second)

		case "SEGUIDOR":
			// Espera um tempo aleatório entre 2 e 4 segundos
			timeout := time.Duration(rand.Intn(2000)+2000) * time.Millisecond
			time.Sleep(timeout)

			c.Mutex.Lock()
			tempoSemOuvirLider := time.Since(c.UltimoContato)
			c.Mutex.Unlock()

			// Se estourou o tempo, o líder caiu! Eu viro candidato.
			if tempoSemOuvirLider >= timeout {
				fmt.Println("\n⚠️ [Raft] Líder sumiu! Iniciando nova eleição...")
				c.Mutex.Lock()
				c.EstadoRaft = "CANDIDATO"
				c.Mutex.Unlock()
			}

		case "CANDIDATO":
			c.Mutex.Lock()
			c.TermoAtual++
			c.VotouEm = c.ID
			meuTermo := c.TermoAtual
			c.UltimoContato = time.Now()
			c.Mutex.Unlock()

			fmt.Printf("👑 [Raft] %s se candidatou para o Termo %d. Pedindo votos...\n", c.ID, meuTermo)
			votos := 1

			// Pedir votos para os outros contêineres
			for _, peer := range c.PeersRPC {
				req := RequisicaoVoto{Termo: meuTermo, CandidatoID: c.ID}
				var res RespostaVoto

				clienteRPC, err := rpc.Dial("tcp", peer)
				if err == nil {
					err = clienteRPC.Call("ServicoRaft.PedirVoto", &req, &res)
					if err == nil && res.VotoConcedido {
						votos++
					}
					clienteRPC.Close()
				}
			}

			// Ganhei a maioria?
			maioria := (len(c.PeersRPC)+1)/2 + 1
			c.Mutex.Lock()
			if votos >= maioria && c.EstadoRaft == "CANDIDATO" {
				fmt.Printf("🎉 [Raft] FUI ELEITO! %s agora é o NOVO LÍDER!\n\n", c.ID)
				c.EstadoRaft = "LIDER"
			} else {
				// Perdi ou empatei, volto a ser seguidor para tentar de novo
				c.EstadoRaft = "SEGUIDOR"
			}
			c.Mutex.Unlock()

			// Pausa curta antes do próximo ciclo
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Callback para quando o drone avisa que terminou o serviço
func (c *Controlador) AoReceberAlertaMQTT(client mqtt.Client, msg mqtt.Message) {
	c.Mutex.Lock()
	if c.EstadoRaft != "LIDER" {
		c.Mutex.Unlock()
		return
	}

	// Decodifica o JSON do Sensor
	var alerta AlertaSensor
	if err := json.Unmarshal(msg.Payload(), &alerta); err != nil {
		c.Mutex.Unlock()
		return
	}

	// Procura um drone livre
	var droneEscolhido *Drone
	for _, d := range c.Frota {
		if d.Status == "LIVRE" {
			droneEscolhido = d
			break
		}
	}
	c.Mutex.Unlock()

	// Despacha ou avisa que não tem drone
	if droneEscolhido != nil {
		req := RequisicaoLog{DroneID: droneEscolhido.ID, Status: "OCUPADO", Setor: alerta.Setor}
		votos := 1
		for _, peer := range c.PeersRPC {
			cliente, err := rpc.Dial("tcp", peer)
			if err == nil {
				var res RespostaLog
				if cliente.Call("ServicoRaft.SincronizarLog", &req, &res) == nil && res.Sucesso {
					votos++
				}
				cliente.Close()
			}
		}

		maioria := (len(c.PeersRPC)+1)/2 + 1
		if votos >= maioria {
			c.Mutex.Lock()
			droneEscolhido.Status = "OCUPADO"
			droneEscolhido.Setor = alerta.Setor
			c.Mutex.Unlock()

			fmt.Printf(" -> ✅ Despachando %s via MQTT para o Setor %s!\n", droneEscolhido.ID, alerta.Setor)

			// Constrói a Ordem de Voo em JSON para o Drone
			ordemJSON := fmt.Sprintf(`{"setor": "%s", "coordenadas": "%s"}`, alerta.Setor, alerta.Coordenadas)
			c.MqttClient.Publish("drones/cmd/"+droneEscolhido.ID, 1, false, ordemJSON)
		}
	} else {
		fmt.Println(" -> ⚠️ Ignorando requisição: Nenhum drone LIVRE no momento!")
	}
}

func (c *Controlador) AoReceberStatusDrone(client mqtt.Client, msg mqtt.Message) {
	c.Mutex.Lock()
	if c.EstadoRaft != "LIDER" {
		c.Mutex.Unlock()
		return
	}
	c.Mutex.Unlock()

	// Decodifica o JSON do Drone avisando que terminou
	var status StatusDrone
	if err := json.Unmarshal(msg.Payload(), &status); err != nil {
		return
	}

	// Dispara o RPC para o cluster avisando que o drone está livre
	req := RequisicaoLog{DroneID: status.DroneID, Status: status.Status, Setor: "base"}
	votos := 1
	for _, peer := range c.PeersRPC {
		cliente, err := rpc.Dial("tcp", peer)
		if err == nil {
			var res RespostaLog
			if cliente.Call("ServicoRaft.SincronizarLog", &req, &res) == nil && res.Sucesso {
				votos++
			}
			cliente.Close()
		}
	}

	// Confirma o reset no próprio caderninho
	maioria := (len(c.PeersRPC)+1)/2 + 1
	if votos >= maioria {
		c.Mutex.Lock()
		if d, ok := c.Frota[status.DroneID]; ok {
			d.Status = status.Status
			d.Setor = "base"
		}
		c.Mutex.Unlock()
		fmt.Printf(" -> 🔄 [Raft] O %s terminou e foi marcado como %s no cluster inteiro!\n", status.DroneID, status.Status)
	}
}

func main() {
	// para a aleatoriedade do tempo funcionar
	rand.Seed(time.Now().UnixNano())

	meuID := os.Getenv("CONTROLLER_ID")
	minhaPorta := os.Getenv("CONTROLLER_PORT")
	peersRaw := os.Getenv("PEERS_RPC")

	listaPeers := []string{}
	if peersRaw != "" {
		listaPeers = strings.Split(peersRaw, ",")
	}

	meuControlador := &Controlador{
		ID:            meuID,
		EstadoRaft:    "SEGUIDOR",
		TermoAtual:    0,
		UltimoContato: time.Now(),
		Frota: map[string]*Drone{
			"drone_01": {ID: "drone_01", Status: "LIVRE", Setor: "base"},
			"drone_02": {ID: "drone_02", Status: "LIVRE", Setor: "base"},
		},
		PeersRPC: listaPeers,
	}

	// Inicia RPC
	servico := &ServicoRaft{C: meuControlador}
	rpc.Register(servico)
	listener, _ := net.Listen("tcp", ":"+minhaPorta)
	go rpc.Accept(listener)

	// Inicia Motor do Raft em paralelo
	go meuControlador.IniciarMotorRaft()

	// Configura MQTT
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://broker-mqtt:1883")
	opts.SetClientID("Raft_" + meuID)
	meuControlador.MqttClient = mqtt.NewClient(opts)

	fmt.Println("[Sistema] Aguardando broker...")
	for {
		if token := meuControlador.MqttClient.Connect(); token.Wait() && token.Error() == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Printf("[MQTT] Conectado! Nós online: %s\n", meuID)

	// Controlador escuta os gritos de socorro dos sensores
	meuControlador.MqttClient.Subscribe("setor/+/emergencia", 1, meuControlador.AoReceberAlertaMQTT)

	// Controlador escuta os drones avisando que terminaram o serviço
	meuControlador.MqttClient.Subscribe("drones/status/+", 1, meuControlador.AoReceberStatusDrone)
	select {}
}
