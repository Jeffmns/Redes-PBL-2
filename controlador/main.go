package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
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

type AlertaSensor struct {
	Setor       string `json:"setor"`
	Coordenadas string `json:"coordenadas"`
	Gravidade   string `json:"gravidade"`
	Timestamp   string `json:"timestamp"`
}

type StatusDrone struct {
	DroneID string `json:"drone_id"`
	Status  string `json:"status"`
}

type RequisicaoFila struct {
	Acao   string
	Alerta AlertaSensor
}
type RespostaFila struct {
	Sucesso bool
}

type RequisicaoLog struct{ DroneID, Status, Setor string }
type RespostaLog struct{ Sucesso bool }

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

type Controlador struct {
	ID            string
	EstadoRaft    string
	TermoAtual    int
	VotouEm       string
	UltimoContato time.Time
	Frota         map[string]*Drone
	FilaPendentes []AlertaSensor
	Mutex         sync.Mutex
	MqttClient    mqtt.Client
	PeersRPC      []string
}

type ServicoRaft struct{ C *Controlador }

// Função auxiliar para calcular o peso da gravidade
func pesoGravidade(g string) int {
	switch g {
	case "ALTA":
		return 3
	case "MEDIA":
		return 2
	case "BAIXA":
		return 1
	default:
		return 0
	}
}

// ==========================================
// 3. O SERVIÇO RPC (As funções chamadas via rede)
// ==========================================
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

func (s *ServicoRaft) SincronizarFila(req *RequisicaoFila, res *RespostaFila) error {
	s.C.Mutex.Lock()
	defer s.C.Mutex.Unlock()

	if req.Acao == "ADICIONAR" {
		s.C.FilaPendentes = append(s.C.FilaPendentes, req.Alerta)

		sort.Slice(s.C.FilaPendentes, func(i, j int) bool {
			pesoA := pesoGravidade(s.C.FilaPendentes[i].Gravidade)
			pesoB := pesoGravidade(s.C.FilaPendentes[j].Gravidade)
			if pesoA != pesoB {
				return pesoA > pesoB
			}
			tempoA, _ := time.Parse(time.RFC3339, s.C.FilaPendentes[i].Timestamp)
			tempoB, _ := time.Parse(time.RFC3339, s.C.FilaPendentes[j].Timestamp)
			return tempoA.Before(tempoB)
		})
		fmt.Printf("[RPC Fila] 📥 Setor %s adicionado à fila replicada!\n", req.Alerta.Setor)

	} else if req.Acao == "REMOVER" && len(s.C.FilaPendentes) > 0 {
		s.C.FilaPendentes = s.C.FilaPendentes[1:]
		fmt.Println("[RPC Fila] 📤 Primeiro item removido da fila replicada!")
	}

	res.Sucesso = true
	return nil
}

func (s *ServicoRaft) Ping(req *RequisicaoPing, res *RespostaPing) error {
	s.C.Mutex.Lock()
	defer s.C.Mutex.Unlock()

	if req.Termo >= s.C.TermoAtual {
		s.C.EstadoRaft = "SEGUIDOR"
		s.C.TermoAtual = req.Termo
		s.C.UltimoContato = time.Now()
	}
	res.Sucesso = true
	return nil
}

func (s *ServicoRaft) PedirVoto(req *RequisicaoVoto, res *RespostaVoto) error {
	s.C.Mutex.Lock()
	defer s.C.Mutex.Unlock()

	if req.Termo < s.C.TermoAtual {
		res.VotoConcedido = false
		return nil
	}

	if req.Termo > s.C.TermoAtual {
		s.C.TermoAtual = req.Termo
		s.C.EstadoRaft = "SEGUIDOR"
		s.C.VotouEm = ""
	}

	if s.C.VotouEm == "" || s.C.VotouEm == req.CandidatoID {
		s.C.VotouEm = req.CandidatoID
		s.C.UltimoContato = time.Now()
		res.VotoConcedido = true
		fmt.Printf("[Raft] Votei no Candidato %s para o Termo %d!\n", req.CandidatoID, req.Termo)
	} else {
		res.VotoConcedido = false
	}
	res.Termo = s.C.TermoAtual
	return nil
}

func (c *Controlador) IniciarMotorRaft() {
	for {
		c.Mutex.Lock()
		estadoAtual := c.EstadoRaft
		c.Mutex.Unlock()

		switch estadoAtual {
		case "LIDER":
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
			timeout := time.Duration(rand.Intn(2000)+2000) * time.Millisecond
			time.Sleep(timeout)

			c.Mutex.Lock()
			tempoSemOuvirLider := time.Since(c.UltimoContato)
			c.Mutex.Unlock()

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

			fmt.Printf("[Raft] %s se candidatou para o Termo %d. Pedindo votos...\n", c.ID, meuTermo)
			votos := 1

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

			maioria := (len(c.PeersRPC)+1)/2 + 1
			c.Mutex.Lock()
			if votos >= maioria && c.EstadoRaft == "CANDIDATO" {
				fmt.Printf("[Raft] FUI ELEITO! %s agora é o NOVO LÍDER!\n\n", c.ID)
				c.EstadoRaft = "LIDER"
			} else {
				c.EstadoRaft = "SEGUIDOR"
			}
			c.Mutex.Unlock()
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// ==========================================
// CALLBACKS MQTT
// ==========================================
func (c *Controlador) AoReceberAlertaMQTT(client mqtt.Client, msg mqtt.Message) {

	fmt.Printf("[DEBUG] Controlador %s ouviu: %s | Conteúdo: %s\n", c.ID, msg.Topic(), string(msg.Payload()))
	c.Mutex.Lock()
	if c.EstadoRaft != "LIDER" {
		c.Mutex.Unlock()
		return
	}

	var alerta AlertaSensor
	if err := json.Unmarshal(msg.Payload(), &alerta); err != nil {
		c.Mutex.Unlock()
		return
	}

	var droneEscolhido *Drone
	for _, d := range c.Frota {
		if d.Status == "LIVRE" {
			droneEscolhido = d
			break
		}
	}
	c.Mutex.Unlock()

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
			ordemJSON := fmt.Sprintf(`{"setor": "%s", "coordenadas": "%s"}`, alerta.Setor, alerta.Coordenadas)
			c.MqttClient.Publish("drones/cmd/"+droneEscolhido.ID, 1, false, ordemJSON)
		}
	} else {
		reqFila := RequisicaoFila{Acao: "ADICIONAR", Alerta: alerta}
		votosFila := 1

		for _, peer := range c.PeersRPC {
			cliente, err := rpc.Dial("tcp", peer)
			if err == nil {
				var resFila RespostaFila
				if cliente.Call("ServicoRaft.SincronizarFila", &reqFila, &resFila) == nil && resFila.Sucesso {
					votosFila++
				}
				cliente.Close()
			}
		}

		maioria := (len(c.PeersRPC)+1)/2 + 1
		if votosFila >= maioria {
			c.Mutex.Lock()
			c.FilaPendentes = append(c.FilaPendentes, alerta)

			sort.Slice(c.FilaPendentes, func(i, j int) bool {
				pesoA := pesoGravidade(c.FilaPendentes[i].Gravidade)
				pesoB := pesoGravidade(c.FilaPendentes[j].Gravidade)
				if pesoA != pesoB {
					return pesoA > pesoB
				}
				tempoA, _ := time.Parse(time.RFC3339, c.FilaPendentes[i].Timestamp)
				tempoB, _ := time.Parse(time.RFC3339, c.FilaPendentes[j].Timestamp)
				return tempoA.Before(tempoB)
			})

			tamanhoFila := len(c.FilaPendentes)
			proximoFila := c.FilaPendentes[0]
			c.Mutex.Unlock()

			fmt.Printf(" -> ⚠️ [Consenso Fila] Tamanho: %d | Próximo: Setor %s (Grav: %s)\n", tamanhoFila, proximoFila.Setor, proximoFila.Gravidade)
		}
	}
}

func (c *Controlador) AoReceberStatusDrone(client mqtt.Client, msg mqtt.Message) {
	c.Mutex.Lock()
	if c.EstadoRaft != "LIDER" {
		c.Mutex.Unlock()
		return
	}
	c.Mutex.Unlock()

	var status StatusDrone
	if err := json.Unmarshal(msg.Payload(), &status); err != nil {
		return
	}

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

	maioria := (len(c.PeersRPC)+1)/2 + 1
	if votos >= maioria {
		c.Mutex.Lock()
		if d, ok := c.Frota[status.DroneID]; ok {
			d.Status = status.Status
			d.Setor = "base"
		}

		// Lógica para pegar da fila e consumir
		temFila := len(c.FilaPendentes) > 0
		var proximoAlerta AlertaSensor

		if temFila {
			proximoAlerta = c.FilaPendentes[0]
			c.FilaPendentes = c.FilaPendentes[1:] // Remove do Líder
		}
		c.Mutex.Unlock()

		fmt.Printf(" -> 🔄 [Raft] O %s terminou e foi marcado como %s!\n", status.DroneID, status.Status)

		if temFila {
			// 1. Avisa os seguidores para removerem o item da fila via RPC
			reqFila := RequisicaoFila{Acao: "REMOVER"}
			for _, peer := range c.PeersRPC {
				go func(p string) {
					cliente, err := rpc.Dial("tcp", p)
					if err == nil {
						cliente.Call("ServicoRaft.SincronizarFila", &reqFila, &RespostaFila{})
						cliente.Close()
					}
				}(peer)
			}

			// 2. Despacha o alerta para ser processado de novo
			fmt.Printf(" -> 🚨 PROCESSANDO FILA: Atendendo Setor %s (Gravidade: %s)!\n", proximoAlerta.Setor, proximoAlerta.Gravidade)
			alertaJSON, _ := json.Marshal(proximoAlerta)
			topico := fmt.Sprintf("setor/%s/emergencia", proximoAlerta.Setor)
			c.MqttClient.Publish(topico, 1, false, alertaJSON)
		}
	}
}

func main() {
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
		FilaPendentes: []AlertaSensor{}, // Inicializa a fila vazia
		PeersRPC:      listaPeers,
	}

	servico := &ServicoRaft{C: meuControlador}
	rpc.Register(servico)
	listener, _ := net.Listen("tcp", ":"+minhaPorta)
	go rpc.Accept(listener)

	go meuControlador.IniciarMotorRaft()

	opts := mqtt.NewClientOptions()
	brokersEnv := os.Getenv("BROKERS")
	if brokersEnv == "" {
		brokersEnv = "tcp://localhost:1883"
	}

	listaBrokers := strings.Split(brokersEnv, ",")
	for _, brokerURL := range listaBrokers {
		opts.AddBroker(brokerURL)
	}
	opts.SetClientID("Raft_" + meuID)
	meuControlador.MqttClient = mqtt.NewClient(opts)

	fmt.Println("[Rede] Tentando conectar ao cluster MQTT...")
	for {
		token := meuControlador.MqttClient.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		fmt.Printf("⚠️ [Rede] Broker indisponível (%v). Tentando em 3s...\n", token.Error())
		time.Sleep(3 * time.Second)
	}
	fmt.Printf("✅ [MQTT] Conectado! Nó online: %s\n", meuID)

	tokenAlerta := meuControlador.MqttClient.Subscribe("setor/+/emergencia", 1, meuControlador.AoReceberAlertaMQTT)
	if tokenAlerta.Wait() && tokenAlerta.Error() != nil {
		fmt.Printf("[DEBUG] Erro ao assinar emergências: %v\n", tokenAlerta.Error())
	} else {
		fmt.Println("[DEBUG] Controlador inscrito com sucesso em 'setor/+/emergencia'")
	}

	tokenDrone := meuControlador.MqttClient.Subscribe("drones/status/+", 1, meuControlador.AoReceberStatusDrone)
	if tokenDrone.Wait() && tokenDrone.Error() != nil {
		fmt.Printf("[DEBUG] Erro ao assinar status dos drones: %v\n", tokenDrone.Error())
	} else {
		fmt.Println("[DEBUG] Controlador inscrito com sucesso em 'drones/status/+'")
	}

	select {}
}
