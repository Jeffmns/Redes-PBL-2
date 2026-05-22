package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Estrutura da ordem que o Controlador enviará
type OrdemDeVoo struct {
	Setor       string `json:"setor"`
	Coordenadas string `json:"coordenadas"`
}

// Estrutura do status que o Drone vai devolver
type StatusDrone struct {
	DroneID string `json:"drone_id"`
	Status  string `json:"status"` // LIVRE, OCUPADO, MANUTENCAO
}

var meuID string
var mqttClient mqtt.Client

// Callback que roda toda vez que o drone recebe uma missão
func aoReceberMissao(client mqtt.Client, msg mqtt.Message) {
	fmt.Println("\n=================================================")
	fmt.Println("📩 [Drone] NOVA MENSAGEM RECEBIDA DO CONTROLADOR!")

	// Decodifica o JSON recebido
	var ordem OrdemDeVoo
	err := json.Unmarshal(msg.Payload(), &ordem)
	if err != nil {
		fmt.Println("❌ [Drone] Erro ao decodificar ordem:", err)
		return
	}

	fmt.Printf("🚀 [Drone] Iniciando missão de resgate!\n   -> Destino: Setor %s\n   -> Coordenadas: %s\n", ordem.Setor, ordem.Coordenadas)

	// Simula o voo e o trabalho (10 segundos)
	fmt.Println("   -> [Drone] Em trânsito e operando... 🚁")
	time.Sleep(10 * time.Second)

	fmt.Println("   -> ✅ [Drone] Missão concluída com sucesso! Retornando à base.")

	// Informa ao Controlador que o drone está livre novamente
	statusFinal := StatusDrone{
		DroneID: meuID,
		Status:  "LIVRE",
	}

	payloadJSON, _ := json.Marshal(statusFinal)
	topicoStatus := fmt.Sprintf("drones/status/%s", meuID)

	fmt.Printf("   -> 📡 [Drone] Avisando a rede no tópico '%s' que estou livre.\n", topicoStatus)
	client.Publish(topicoStatus, 1, false, payloadJSON)
	fmt.Println("=================================================")
}

func main() {
	meuID = os.Getenv("DRONE_ID")
	if meuID == "" {
		meuID = "drone_desconhecido"
	}

	opts := mqtt.NewClientOptions()
	// Leia a variável BROKERS do ambiente (lembre-se de importar o pacote "strings")
	brokersEnv := os.Getenv("BROKERS")
	if brokersEnv == "" {
		brokersEnv = "tcp://localhost:1883" // Fallback seguro
	}

	// Separe pela vírgula e adicione cada um deles nas opções do MQTT
	listaBrokers := strings.Split(brokersEnv, ",")
	for _, brokerURL := range listaBrokers {
		opts.AddBroker(brokerURL)
	}
	opts.SetClientID("Cliente_MQTT_" + meuID)

	mqttClient = mqtt.NewClient(opts)
	fmt.Println("[Rede] Tentando conectar ao cluster MQTT...")
	for {
		token := mqttClient.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}

		fmt.Printf("⚠️ [Rede] Broker ainda indisponível (%v). Tentando novamente em 3s...\n", token.Error())
		time.Sleep(3 * time.Second)
	}
	fmt.Println("✅ [Rede] Conectado ao cluster MQTT com sucesso!")

	topicoComando := fmt.Sprintf("drones/cmd/%s", meuID)

	mqttClient.Subscribe(topicoComando, 1, aoReceberMissao)

	fmt.Printf("🚁 [Drone %s] Sistemas Online. Aguardando despachos no tópico '%s'...\n", meuID, topicoComando)

	select {}
}
