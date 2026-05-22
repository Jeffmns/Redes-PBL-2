package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Estrutura de dados que será convertida para JSON
type AlertaSensor struct {
	Setor       string `json:"setor"`
	Coordenadas string `json:"coordenadas"`
	Gravidade   string `json:"gravidade"`
	Timestamp   string `json:"timestamp"`
}

func main() {
	nomeSetor := os.Getenv("SETOR_NOME")
	if nomeSetor == "" {
		nomeSetor = "desconhecido"
	}

	opts := mqtt.NewClientOptions()
	// 1. Leia a variável BROKERS do ambiente (lembre-se de importar o pacote "strings")
	brokersEnv := os.Getenv("BROKERS")
	if brokersEnv == "" {
		brokersEnv = "tcp://localhost:1883" // Fallback seguro
	}

	// 2. Separe pela vírgula e adicione cada um deles nas opções do MQTT
	listaBrokers := strings.Split(brokersEnv, ",")
	for _, brokerURL := range listaBrokers {
		opts.AddBroker(brokerURL)
	}
	// Usa o nome do setor para o Client ID não dar conflito
	opts.SetClientID("Sensor_Setor_" + nomeSetor)

	client := mqtt.NewClient(opts)
	fmt.Println("[Rede] Tentando conectar ao cluster MQTT...")
	for {
		token := client.Connect()
		token.Wait()
		if token.Error() == nil {
			break // Conexão bem-sucedida! Quebra o loop e continua o programa.
		}

		fmt.Printf("⚠️ [Rede] Broker ainda indisponível (%v). Tentando novamente em 3s...\n", token.Error())
		time.Sleep(3 * time.Second)
	}
	fmt.Printf("📡 [Sensor] Conectado ao Broker! Monitorando Setor %s...\n", nomeSetor)

	for {
		espera := time.Duration(rand.Intn(20)+15) * time.Second
		time.Sleep(espera)

		lat := -12.0 + rand.Float64()
		lon := -38.0 + rand.Float64()
		gravidades := []string{"BAIXA", "MEDIA", "ALTA"}

		alerta := AlertaSensor{
			Setor:       nomeSetor,
			Coordenadas: fmt.Sprintf("%.4f, %.4f", lat, lon),
			Gravidade:   gravidades[rand.Intn(len(gravidades))],
			Timestamp:   time.Now().Format(time.RFC3339),
		}

		payloadJSON, _ := json.Marshal(alerta)

		// Monta o tópico dinamicamente: setor/sul/emergencia, setor/leste/emergencia...
		topico := fmt.Sprintf("setor/%s/emergencia", alerta.Setor)
		fmt.Printf("\n🚨 [Sensor %s] Anomalia detectada!\n   -> Publicando em: %s\n", nomeSetor, topico)

		client.Publish(topico, 1, false, payloadJSON)
	}
}
