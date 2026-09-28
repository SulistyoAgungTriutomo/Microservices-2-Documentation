package main

import (
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PaymentMessage struct {
	PaymentID string `json:"payment_id"`
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
	CreatedAt string `json:"created_at"`
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Gagal terhubung ke RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Gagal membuka channel: %v", err)
	}
	defer ch.Close()

	// Pastikan queue tersertifikasi
	q, err := ch.QueueDeclare(
		"payment.queue",
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Fatalf("Gagal mendeklarasikan queue: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer tag
		false,  // auto-ack (diatur false agar kita ack manual setelah diproses)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,
	)
	if err != nil {
		log.Fatalf("Gagal mendaftarkan consumer: %v", err)
	}

	log.Println("[Payment-Service] Menunggu pesan dari payment.queue...")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var msg PaymentMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				log.Printf("Gagal membaca payload pesan: %v", err)
				d.Nack(false, false) // Buang pesan jika format salah
				continue
			}

			createdAt, err := time.Parse(time.RFC3339, msg.CreatedAt)
			if err != nil {
				log.Printf("Format timestamp salah: %v", err)
				d.Nack(false, false)
				continue
			}

			// Cek apakah pesan sudah kedaluwarsa (> 10 menit)
			if time.Since(createdAt) > 10*time.Minute {
				log.Printf("[EXPIRED] Payment ID: %s kedaluwarsa (dibuat pada %s)", msg.PaymentID, msg.CreatedAt)
				d.Ack(false) // Tandai pesan selesai diproses (sebagai expired)
				continue
			}

			// Simulasi pemrosesan pembayaran
			log.Printf("[PROCESSING] Memproses Payment ID: %s sebesar Rp%d untuk User ID: %s...", msg.PaymentID, msg.Amount, msg.UserID)
			time.Sleep(2 * time.Second) // Simulasi delay proses

			log.Printf("[SUCCESS] Payment ID: %s berhasil diproses", msg.PaymentID)

			// Konfirmasi pesan telah sukses diproses
			d.Ack(false)
		}
	}()

	<-forever
}
