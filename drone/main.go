package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
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
	opts.AddBroker("tcp://broker-mqtt:1883")
	opts.SetClientID("Cliente_MQTT_" + meuID)

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Erro ao conectar no broker: %v", token.Error())
	}

	topicoComando := fmt.Sprintf("drones/cmd/%s", meuID)

	mqttClient.Subscribe(topicoComando, 1, aoReceberMissao)

	fmt.Printf("🚁 [Drone %s] Sistemas Online. Aguardando despachos no tópico '%s'...\n", meuID, topicoComando)

	select {}
}
