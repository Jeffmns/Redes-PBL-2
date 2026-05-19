package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Estrutura de dados que será convertida para JSON
type AlertaSensor struct {
	Setor       string `json:"setor"`
	Coordenadas string `json:"coordenadas"`
	Gravidade   string `json:"gravidade"` // ALTA, MEDIA, BAIXA
	Timestamp   string `json:"timestamp"`
}

func main() {
	nomeSetor := os.Getenv("SETOR_NOME")
	if nomeSetor == "" {
		nomeSetor = "desconhecido"
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://broker-mqtt:1883")
	// Usa o nome do setor para o Client ID não dar conflito
	opts.SetClientID("Sensor_Setor_" + nomeSetor)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Erro ao conectar no broker: %v", token.Error())
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
